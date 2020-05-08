#!/usr/bin/python
import boto3
import os
import sys
import time
from argparse import *

region=os.environ.get('EC2_REGION')
az=os.environ.get('EC2_AVAILABILITY_ZONE')
instanceid=os.environ.get('EC2_INSTANCE_ID')
asg=os.environ.get('NETFLIX_AUTO_SCALE_GROUP')

ebs_alias='xvdf'
ebs_path = '/tmp/ebs-vol-exists'

#size=50 # size of the ebs volume
#iops= min(size*50, 64000)

class EbsHelper():
    """
    script to handle ebs create, delete, attach and detach operations.
    currently the mapping information is not stored.
    """

    def __init__(self, action_type, vol_id=None, size=50, asg=None):
        self.action_type = action_type
        self.vol_id = vol_id
	self.size = size
	if self.size == None:
	    self.size = 50
	self.iops = min(self.size*50, 64000)
        self.asg = asg

    def takeAction(self):
        if self.action_type == "create-attach-vol":
            self.createAttachEBS()
        elif self.action_type == "detach-delete-vol":
            self.detachDeleteEBS()
        elif self.action_type == "detach-vol":
            self.detachEBS()
        elif self.action_type == "delete-vol":
            self.deleteEBS()
        elif self.action_type == "create-vol":
            self.createEBS()
        elif self.action_type == "attach-vol-format":
            self.attachEBSFormat()
        elif self.action_type == "attach-vol-ro":
            self.attachEBSRO()
        elif self.action_type == "attach-vol-rw":
            self.attachEBSRW()
        elif self.action_type == "get-vol":
            self.getVolIdEBS()
        elif self.action_type == "list-vol":
            self.listEBS()
        else:
            print('unsupported action type. please run --help for valid actions')
            sys.exit(1)

    def createAttachEBS(self):
        ebs_vol_exists = os.path.isfile(ebs_path)
        if ebs_vol_exists:
            print("ebs volume is already created for this node")
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volId = fileinfo.split()[0]
                print(volId)
            return

        client = boto3.client('ec2', region_name=region)
        response = client.create_volume(
                            AvailabilityZone=az,
                            Encrypted=False,
                            Iops=self.iops,
                            Size=self.size,
                            VolumeType='io1',
                            DryRun=False,
                            MultiAttachEnabled=True,
                            TagSpecifications=[
                                dict(
                                    ResourceType="volume",
                                    Tags=[
                                       dict(Key="cde:technology", Value="evcache"),
                                       dict(Key="cde:asg", Value=asg)
                            ])]
                    )
        print(response)

        volId=response['VolumeId']

        ec2 = boto3.resource('ec2', region_name=region)
        volume = ec2.Volume(volId)
        print(volume.state, volume.volume_type, volume.volume_id)

        while volume.state != 'available':
            print("volume creation in progress", volId, volume.state)
            time.sleep(5)
            volume.load()

        print("volume is now available", volume.volume_id, volume.state)
        with open(ebs_path, 'w') as f:
            f.write(volume.volume_id+ " " +az)

        print("proceeding with attachment..")

        response=volume.attach_to_instance(Device=ebs_alias,
                                           InstanceId=instanceid)

        print response
        volume.load()
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'in-use':
            print("still attaching..", volume.state)
            time.sleep(5)
            volume.load()

        time.sleep(5)
        print("attachment of volume is done", volume.state)
        print("format and mount the disk")
        os.system("sudo mkdir -p /t/ebs-mnt")
        os.system("sudo mkfs.xfs /dev/"+ebs_alias)
        os.system("sudo mount /dev/"+ebs_alias +" /t/ebs-mnt")
        print("mounted the device at /t/ebs-mnt")

    def createEBS(self):
        ebs_vol_exists = os.path.isfile(ebs_path)
        if ebs_vol_exists:
            print("ebs volume is already created for this node")
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volId = fileinfo.split()[0]
                print(volId)
            return

        client = boto3.client('ec2', region_name=region)
        response = client.create_volume(
                            AvailabilityZone=az,
                            Encrypted=False,
                            Iops=self.iops,
                            Size=self.size,
                            VolumeType='io1',
                            DryRun=False,
                            MultiAttachEnabled=True
                    )
        print(response)

        volId=response['VolumeId']

        ec2 = boto3.resource('ec2', region_name=region)
        volume = ec2.Volume(volId)
        print(volume.state, volume.volume_type, volume.volume_id)

        while volume.state != 'available':
            print("volume creation in progress", volId, volume.state)
            time.sleep(5)
            volume.load()

        print("volume is now available", volume.volume_id, volume.state)
	with open(ebs_path, 'w') as f:
            f.write(volume.volume_id+ " " +az)

    def detachDeleteEBS(self):
        print("unmount the volume")
        os.system("sudo umount -l /t/ebs-mnt")
        ec2 = boto3.resource('ec2', region_name=region)
        instance=ec2.Instance(instanceid)
        block_dev_list=instance.block_device_mappings
        #print(block_dev_list)
        #print(len(block_dev_list))
        volId=None
        for dev in block_dev_list:
            if dev['DeviceName'] == ebs_alias:
                volId=dev['Ebs']['VolumeId']

        if volId == None:
            print("volume is already detached")
            self.deleteEBS()
            return

        volume = ec2.Volume(volId)
        print(volume.state, volume.volume_type, volume.volume_id)

        response = volume.detach_from_instance(
                        Device=ebs_alias,
                        Force=True,
                        InstanceId=instanceid,
                   )
        print(response)

        volume.load()
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'available':
            print("still detaching..", volume.state)
            time.sleep(5)
            volume.load()

        print("detachment of volume is done", volume.state)

    
        ebs_vol_exists = os.path.isfile(ebs_path)
        volume_from_path = None
        if ebs_vol_exists:
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volume_from_path = fileinfo.split()[0]

        print(volume_from_path)

        print("delete the volume")
        client = boto3.client('ec2', region_name=region)
        response = client.delete_volume(VolumeId=volId)
        print(response)
        os.unlink(ebs_path)

    def attachEBSFormat(self):
        ebs_vol_exists = os.path.isfile(ebs_path)
        volume_from_path = None
        if ebs_vol_exists:
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volume_from_path = fileinfo.split()[0]

        ec2 = boto3.resource('ec2', region_name=region)
        volume = ec2.Volume(volume_from_path)

        print("proceeding with attachment..")

        response=volume.attach_to_instance(Device=ebs_alias,
                                           InstanceId=instanceid)

        print response
        volume.load()
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'in-use':
            print("still attaching..", volume.state)
            time.sleep(5)
            volume.load()

        print("attachment of volume is done", volume.state)
        time.sleep(5)
        print("format and mount the disk")
        os.system("sudo mkdir -p /t/ebs-mnt")
        os.system("sudo mkfs.xfs /dev/"+ebs_alias)
        os.system("sudo mount /dev/"+ebs_alias +" /t/ebs-mnt")
        print("mounted the device at /t/ebs-mnt")

    def attachEBSRO(self):
        ebs_vol_exists = os.path.isfile(ebs_path)
        volume_from_path = None
        if ebs_vol_exists:
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volume_from_path = fileinfo.split()[0]
        else:
            # need to get a volume which is not already attached.
            volume_from_path = self.getFreeEBS()

        ec2 = boto3.resource('ec2', region_name=region)
        volume = ec2.Volume(volume_from_path)

        print("proceeding with attachment..")

        response=volume.attach_to_instance(Device=ebs_alias,
                                           InstanceId=instanceid)

        print response
        volume.load()
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'in-use':
            print("still attaching..", volume.state)
            time.sleep(5)
            volume.load()

        print("attachment of volume is done", volume.state)
        time.sleep(5)
        os.system("sudo mkdir -p /t/ebs-mnt")
        os.system("sudo mount -o ro /dev/"+ebs_alias +" /t/ebs-mnt")
        print("mounted the device at /t/ebs-mnt")

    def attachEBSRW(self):
        ebs_vol_exists = os.path.isfile(ebs_path)
        volume_from_path = None
        if ebs_vol_exists:
            with open(ebs_path, 'r') as f:
                fileinfo = f.read()
                volume_from_path = fileinfo.split()[0]

        ec2 = boto3.resource('ec2', region_name=region)
        volume = ec2.Volume(volume_from_path)

        print("proceeding with attachment..")

        response=volume.attach_to_instance(Device=ebs_alias,
                                           InstanceId=instanceid)

        print response
        volume.load()
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'in-use':
            print("still attaching..", volume.state)
            time.sleep(5)
            volume.load()

        print("attachment of volume is done", volume.state)
        time.sleep(5)
        os.system("sudo mount /dev/"+ebs_alias +" /t/ebs-mnt")
        print("mounted the device at /t/ebs-mnt")

    def detachEBS(self):
        print("unmount the volume")
        os.system("sudo umount -l /t/ebs-mnt")
        ec2 = boto3.resource('ec2', region_name=region)
        instance=ec2.Instance(instanceid)
        block_dev_list=instance.block_device_mappings
        print(block_dev_list)
        #print(len(block_dev_list))
        volId=None
        for dev in block_dev_list:
            if dev['DeviceName'] == ebs_alias:
                volId=dev['Ebs']['VolumeId']

        volume = ec2.Volume(volId)
        print(volume.state, volume.volume_type, volume.volume_id)

        response = volume.detach_from_instance(
                        Device=ebs_alias,
                        Force=True,
                        InstanceId=instanceid,
                   )
        print(response)

        volume.load()
        num_of_attachments = len(volume.attachments)
        # Poll whether the volume transitioned to 'in-use' or not.
        while volume.state != 'available':
            print("still detaching..", volume.state)
            time.sleep(5)
            volume.load()
            if num_of_attachments-1 == len(volume.attachments):
                break

        print("detachment of volume is done", volume.state)
        return volume.volume_id

    def deleteEBS(self):
        if self.vol_id == None:
            ebs_vol_exists = os.path.isfile(ebs_path)
            if ebs_vol_exists:
                with open(ebs_path, 'r') as f:
                    fileinfo = f.read()
                    self.vol_id = fileinfo.split()[0]
        if self.vol_id == None:
            print("all volumes associated with this instance are deleted")
            return
        print("delete the volume", self.vol_id)
        client = boto3.client('ec2', region_name=region)
        response = client.delete_volume(VolumeId=self.vol_id)
        print(response)
        os.unlink(ebs_path)

    def getVolIdEBS(self):
        ec2 = boto3.resource('ec2', region_name=region)
        instance=ec2.Instance(instanceid)
        block_dev_list=instance.block_device_mappings
        #print(block_dev_list)
        volId=None
        for dev in block_dev_list:
            if dev['DeviceName'] == ebs_alias:
                volId=dev['Ebs']['VolumeId']
        
        if volId == None:
            ebs_vol_exists = os.path.isfile(ebs_path)
            if ebs_vol_exists:
                with open(ebs_path, 'r') as f:
                    fileinfo = f.read()
                    volId = fileinfo.split()[0]

        print(volId)
        return volId

    def listEBS(self):
        ec2 = boto3.client('ec2', region_name=region)
        vols=ec2.describe_volumes(
                Filters=[
                        {
                            'Name': 'tag:cde:technology',
                            'Values': [
                                    'evcache',
                            ]   
                        },
                        {
                            'Name': 'tag:cde:asg',
                            'Values': [
                                    self.asg,
                            ]   
                        }
                ])
        for vol in vols['Volumes']:
            print vol
            if vol['Attachments'] != None:
                num_attachments = len(vol['Attachments'])
                print(vol['VolumeId'] + " is attached to " + str(num_attachments) + " instance(s)")
                num_attachments = num_attachments-1
                while (num_attachments >= 0):
                    print(vol['Attachments'][num_attachments]['InstanceId'])
                    num_attachments = num_attachments-1
            else:
                print(vol['VolumeId'] + "is not attached to any instance(s)")

    def getFreeEBS(self):
        ec2 = boto3.client('ec2', region_name=region)
        vols=ec2.describe_volumes(
                Filters=[
                        {
                            'Name': 'tag:cde:technology',
                            'Values': [
                                    'evcache',
                            ]   
                        },
                        {
                            'Name': 'tag:cde:asg',
                            'Values': [
                                    self.asg,
                            ]   
                        }
                ])
        for vol in vols['Volumes']:
            if vol['Attachments'] != None:
                num_attachments = len(vol['Attachments'])
                print(vol['VolumeId'] + " is attached to " + str(num_attachments) + " instance(s)")
                if num_attachments == 1:
                    return vol['VolumeId']
                num_attachments = num_attachments-1
                while (num_attachments >= 0):
                    print(vol['Attachments'][num_attachments]['InstanceId'])
                    num_attachments = num_attachments-1
            else:
                print(vol['VolumeId'] + "is not attached to any instance(s)")


def main():
    parser = ArgumentParser(description="Arguments required for configuring ebs",formatter_class=RawTextHelpFormatter)
    parser.add_argument('--action_type', type=str,
      help='actions can be create-attach-vol, detach-delete-vol, detach-vol, delete-vol, attach-vol-format, attach-vol-ro, attach-vol-rw, create-vol, get-vol, list-vol', required=True)
    parser.add_argument('--volume_name', type=str,
      help='name of the volume', required=False)
    parser.add_argument('--size', type=int,
      help='size of the volume in gb', required=False)
    parser.add_argument('--asg', type=str,
      help='source asg', required=False)
    args = parser.parse_args()

    if args.action_type == "attach-vol-ro" and args.asg == None:
        print("source asg needs to be specified when mounting the ebs as ro")
        sys.exit(1)

    ebsh = EbsHelper(args.action_type, args.volume_name, args.size, args.asg)
    ebsh.takeAction()
    return

if __name__=='__main__':
    main()
