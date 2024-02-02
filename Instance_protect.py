#ref : https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ec2.html

import boto3
from botocore.exceptions import ClientError
import sys

def instance_protect(cnetid):
    cnetid_a3 = cnetid+'-a3'
    # get the client
    ec2_client = boto3.client('ec2')

    #find instance by Filters
    instance = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'tag:Name',
                    'Values': [cnetid_a3]
                }
            ]
        )
    for reservation in instance['Reservations']:
        for instance in reservation['Instances']:
            instance_id = instance['InstanceId']
            launch_time = instance['LaunchTime']
            availability_zone = instance['Placement']['AvailabilityZone']

            print('Instance ID: {}'.format(instance_id))
            print('Launch Time: {} (Availability Zone: {})'.format(launch_time,availability_zone))



            #check termination protection status
            attribute = ec2_client.describe_instance_attribute(InstanceId=instance_id, Attribute='disableApiTermination')
            termination_protection = attribute['DisableApiTermination']['Value']

            #enable termination protection if it's not set

            if not termination_protection:
                print(f'This instance does not have termination protection set...Enabling termination protection for instance {instance_id}...')
                ec2_client.modify_instance_attribute(InstanceId=instance_id, Attribute='disableApiTermination', Value='true')
                print(f'Instance {instance_id} now has termination protection set...')
            #attemp to terminate instance

            try:
                ec2_client.terminate_instances(InstanceIds=[instance_id])
                print(f'Instance {instance_id} has been Terminated')
            except ClientError as e:
                print(f'Instance termination failed: The instance {instance_id} may not be terminated. Modify its \'disableApiTermination\' instance attribute and try again')
            print()

def ebs_profile():
    #get the size of each ec2 resource's volume
    ec2_resource = boto3.resource('ec2')
    volumes = [volume.size for volume in ec2_resource.volumes.all()]

    #print Number 
    print('Number of EBS Volumes:', len(volumes))

    #print Total
    print('Total provisioned storage (GB):', sum(volumes))
    print()

def security_group_rules(security_group_name):
    ec2_client=boto3.client('ec2')

    #retrive the IP
    try:
        # retrieve security groups
        response = ec2_client.describe_security_groups(GroupNames=[security_group_name])

        if response['SecurityGroups']:
            # get the first security group
            group = response['SecurityGroups'][0] 
            print("Port           Inbound IP Address Range(s)")
            print("-------------- ---------------------------")
            for i in group['IpPermissions']:
                # extract port range
                from_port = i.get('FromPort', '')
                to_port = i.get('ToPort', '')
                port_range = f"{from_port} - {to_port}"
                
                # extract IPv4 and IPv6 ranges
                ipv4_ranges = [ip_range['CidrIp'] for ip_range in i.get('IpRanges', [])]
                ipv6_ranges = [ip_range['CidrIpv6'] for ip_range in i.get('Ipv6Ranges', [])]

                # print the output
                print(f"{port_range:<15} {ipv4_ranges}{ipv6_ranges}")
    except ClientError as e:
        print(f"Error: {e}")

def main():
    if len(sys.argv) != 3:
        print("Error: Incorrect number of arguments provided.")
        sys.exit(1)

    filter_string = sys.argv[1]
    security_group_name = sys.argv[2]

     # call the functions with the provided arguments

    instance_protect(filter_string)
    ebs_profile()
    security_group_rules(security_group_name)

if __name__ == "__main__":
    main()


