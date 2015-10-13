#!/usr/bin/python

import json, sys, os, re
import time
import logging
from psphere import client
from psphere import managedobjects
from psphere import errors

logger = logging.getLogger(__name__)

def _mk_vmrelocatespec(vsclient, host, dsname):
    datastore = find_datastore(host, dsname)
    if datastore is None:
        raise Exception('Could not find datastore [%s] on host [%s]' %(dsname, host.name))
    spec = vsclient.create('VirtualMachineRelocateSpec')
    spec.datastore = datastore
    spec.host = host
    spec.pool = host.parent.resourcePool
    spec.diskMoveType = 'moveAllDiskBackingsAndDisallowSharing'
    spec.transform = None
    return spec

def find_datastore(host, dsname, require_access=True):
    for ds in host.datastore:
        if ds.summary.name == dsname:
            if require_access and not ds.summary.accessible:
                raise ValueError('Datastore [%s] is not accessible from host [%s]' %(dsname, host.name))
            return ds
    return None

def _mk_relocspec_disklocator(vsclient, ds, disk, vmdktype):
    _, filepath = parse_vmdk_path(disk['vmdkpath'])
    vmdkpath = '[%s] %s' % (ds.name, filepath)
    locator = vsclient.create('VirtualMachineRelocateSpecDiskLocator')
    locator.datastore = ds
    locator.diskId = disk['key']
    def _mk_diskbackinginfo(vsclient, vmdkpath, vmdktype):
        backing = vsclient.create('VirtualDiskFlatVer2BackingInfo')
        backing.diskMode = 'presistent'
        backing.fileName = vmdkpath
        if vmdktype is not None:
            backing.thinProvisioned = vmdktype == disktype.thin
            if vmdktype in [disktype.thickeagerzero, disktype.thicklazyzero]:
                backing.eagerlyScrub = vmdktype == disktype.thickeagerzero
        return backing
    locator.diskBackingInfo = _mk_diskbackinginfo(vsclient, vmdkpath, vmdktype)
    return locator

def find_scsi_controller(vm, scsibusnum):
    for dev in vm.config.hardware.device:
        if dev.deviceInfo.label == 'SCSI controller %d' %scsibusnum:
            return dev
    return None

def find_target(vm, scsibusnum, scsiunitnum):
    controller = find_scsi_controller(vm, scsibusnum)
    for dev in vm.config.hardware.device:
        if getattr(dev, 'controllerKey', None) and \
            dev.controllerKey == controller.key and \
            dev.unitNumber == scsiunitnum:
            return dev
    return None

def get_scsi_controllers(vm):
    controllernos = [0, 1, 2, 3]
    controllers = [find_scsi_controller(vm, x) for x in controllernos]
    return dict([(x.key, x) for x in controllers if x is not None])

def get_vm_disk_control(vm):
    disks = []
    controllers = get_scsi_controllers(vm)
    for dev in vm.config.hardware.device:
        if hasattr(dev, 'controllerKey') and dev.controllerKey in controllers:
            controller = controllers[dev.controllerKey]
            disks.append((controller, dev))
    return disks

def get_vm_disks(vm):
    diskinfo = []
    for controller, disk in get_vm_disk_control(vm):
        diskinfo.append({'label': str(disk.deviceInfo.label),
            'vmdkpath': str(disk.backing.fileName),
            'targetid': '%d:%d' % (controller.busNumber, disk.unitNumber),
            'key': str(disk.key)})
    return diskinfo

def parse_vmdk_path(path):
    m = re.match(r'(\[[^\]]+\]) ([\S]+)', path)
    if not m:
        raise ValueError('%s is not a valid VMDK path' % str(path))
        ds = m.group(1)
        datastore = ds[1:-1]
    return datastore, m.group(2)

def create_snapshot(vsclient, host, vmname):
    from datetime import datetime
    snapname = vmname + datetime.now().strftime('-%Y%m%d %H:%M:%S-snapshot')
    vm = managedobjects.VirtualMachine.get(vsclient, name=vmname)
    try:
        vm.CreateSnapshot_Task(name=snapname, memory=False, quiesce=True)
    except Exception as exc:
        raise "Cannot create snapshot %s" %exc.message
    
class pspherewrapper(object):
    def __init__(self, vcserver, hostname, datastore, username, pwd):
        self.vsclient = client.Client(vcserver, username, pwd)
        self.hostsystem = managedobjects.HostSystem
        self.datastore = datastore
        self.host = managedobjects.HostSystem.get(self.vsclient, name=hostname)
        self.clonespec = self.vsclient.create('VirtualMachineCloneSpec')
        self.configspec = self.vsclient.create('VirtualMachineConfigSpec')

    def find_hosts(self):
        hslist = self.hostsystem.all(self.vsclient)
        #return hslist
        for hs in hslist:
            print hs.name
    def list_vms(self, host_system):
        out = managedobjects.VirtualMachine.all(self.vsclient)
        for vm in out:
            if vm.name == 'tnvm1': print vm

    def create_fullclone(self, srcvmname, newvmname, vmdktype=None):
        vm = managedobjects.VirtualMachine.get(self.vsclient, name=srcvmname)
        relocspec = _mk_vmrelocatespec(self.vsclient, self.host, self.datastore)
        #relocspec.diskMoveType = 'createNewChildDiskBacking'
        if vmdktype is not None:
            relocspec.disk = [_mk_relocspec_disklocator(self.vsclient, relocspec.datastore, disk, vmdktype) for disk in get_vm_disks(vm)]
        self.configspec.name = newvmname
        self.configspec.guestId = vm.config.guestId
        self.configspec.memoryMB = None
        self.configspec.numCPUs = None
        #self.configspec.numCoresPerSocket = None
        self.clonespec.powerOn = False
        self.clonespec.template = False
        self.clonespec.location = relocspec
        self.clonespec.config = self.configspec 
        self.clonespec.customization = None
        self.clonespec.snapshot = None
        vm.CloneVM_Task(name=newvmname, folder=vm.parent,  spec=self.clonespec) 

    def create_linkedclone(self, srcvmname, newvmname):
        vm = managedobjects.VirtualMachine.get(self.vsclient, name=srcvmname)
        create_snapshot(self.vsclient, self.host, vm.name)
        relocspec = _mk_vmrelocatespec(self.vsclient, self.host, self.datastore)
        relocspec.diskMoveType = 'createNewChildDiskBacking'
        self.clonespec.powerOn = False
        self.clonespec.template = False
        self.clonespec.location = relocspec
        self.clonespec.customization = None
        self.clonespec.snapshot = vm.snapshot.currentSnapshot
        vm.CloneVM_Task(name=newvmname, folder=vm.parent,  spec=self.clonespec) 
    
    def delete_vm(self, vmname):
        vm = managedobjects.VirtualMachine.get(self.vsclient, name=vmname)
        return vm.Destroy_Task()

if __name__ == "__main__":
    """
    self test program, 
    clone VMs from one of the parent VM
    present in the datastore

    create instanace of pspherewrapper class
    with inputs as here,

    inst = pspherewrapper('<vcenter-ip or hostname>', 
                        '<esx server-ip or hostname>',
                        '<datastore-to-deploy-vms>', 
                        '<vsphere-client username>', 
                        '<vsphere-client pwd>')
    Sample code to create 25 VMS
    -----------------------------
    for i in range(1, 25):
        #inst.delete_vm('a-tnvm-' + str(i))
        #logger.debug("Creating VM: %s" %str(i))
        time.sleep(10)
        inst.create_fullclone('a-tnvm-0', 'a-tnvm-' + str(i)) #call create_linkedclone if you need to create linkedclones

    Sample code to create 25 VMS
    -----------------------------
    for i in range(1, 25):
        logger.debug("Deleting VM: %s" %str(i))
        inst.delete_vm('a-tnvm-' + str(i))
    """
    pass
