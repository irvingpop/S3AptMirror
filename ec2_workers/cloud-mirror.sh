#!/bin/bash -x
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
EMAIL=me@domain.com
instance_id=$(wget -qO- instance-data/latest/meta-data/instance-id)
public_ip=$(wget -qO- instance-data/latest/meta-data/public-ipv4)
zone=$(wget -qO- instance-data/latest/meta-data/placement/availability-zone)
region=$(expr match $zone '\(.*\).')
uptime=$(uptime)
failed="False"

export ACCESS_KEY="AKEY"
export SECRET_KEY="SKEY"

fail() {
	/usr/sbin/sendmail -oi -t -f ${EMAIL} <<EOM
From: ${EMAIL}
To: ${EMAIL}
Importance: High
Subject: EC2 S3 Mirror syncornization failure in ${region}

This email message was generated on the following EC2 instance:

  instance id: ${instance_id}
  region:      ${region}
  public ip:   ${public_ip}
  uptime:      ${uptime}

The mirror syncronization job running in ${region} has failed.

The cause of the failure is:
${1}

The instance will remain up for 1hr or until it is cleaned-up
by the Autoscaling group. You may attempt to access it using the
SSH Key called "worker-ssh-key", i.e.:
    ssh -i worker-ssh-key ubuntu@${public_ip}

Instance Logs:
$(cat /var/log/user-data.log)

EOM
	sleep 3600
	exit 1
}

bzr branch lp:s3aptmirror /tmp/s3aptmirror

[ -e "/tmp/s3aptmirror" ] ||
	fail "/tmp/s3aptmirror is missing. This does not affect the mirror integrity, however the synchronization did not happen"

{
	cd /tmp/s3aptmirror

	while /bin/true
	do
		python -OO apt2s3mirror --destination ${region}.ec2.archive.ubuntu.com --access_key ${ACCESS_KEY} --secret_key ${SECRET_KEY} --purge_old --delete_delay 3 --server archive.ubuntu.com
		error=$?

		if [ ${error} -ne 3 -o ${error} -ne 0 ]; then
			fail "Synchronization failed for Archive. Please see logs. Mirror integrity may have been compromised."
		else
			echo "Restarting, detected meta-data change in the middle of processing"
		fi

	done
}

sleep 500
shutdown -h now
exit 0
