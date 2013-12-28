#!/bin/bash

for line in $( sed -e 's,",,g' keys.txt )
do
	region=$(echo ${line} | sed -e 's,worker-,,g' | awk -F, '{print$1}')
	access_key=$(echo ${line} | awk -F, '{print$2}')
	secret_key=$(echo ${line} | awk -F, '{print$3}')

	sed -e "s,AKEY,${access_key},g" -e "s,SKEY,${secret_key},g" cloud-mirror.sh > cloud-mirror-${region}.sh

	write-mime-multipart --output WORKER-${region}.cfg \
		cloud-config.txt \
		config-postfix.sh \
		cloud-mirror-${region}.sh

	rm cloud-mirror-${region}.sh

done
