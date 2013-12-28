#!/bin/bash
hostname=$(wget -qO- instance-data/latest/meta-data/public-hostname)
cat << EOC > /etc/postfix/main.cf
smtpd_banner = \$myhostname ESMTP \$mail_name (Ubuntu)
biff = no
append_dot_mydomain = no
readme_directory = no
smtp_tls_CAfile = /etc/ssl/certs/ca-certificates.crt
smtpd_use_tls=yes
smtpd_tls_session_cache_database = btree:\${data_directory}/smtpd_scache
smtp_tls_session_cache_database = btree:\${data_directory}/smtp_scache
masquerade_domains = ${hostname} 
masquerade_exceptions = root
myhostname = ${hostname}
alias_maps = hash:/etc/aliases
alias_database = hash:/etc/aliases
myorigin = /etc/mailname
mydestination = padfoot, localhost
mynetworks = 127.0.0.0/8 [::ffff:127.0.0.0]/104 [::1]/128
mailbox_size_limit = 0
recipient_delimiter = +
inet_interfaces = loopback-only
relay_domains = DOMAIN
relayhost = [SERVER]:587
smtp_sasl_auth_enable = yes
smtp_use_tls = yes
smtp_sasl_password_maps = PASSWORD 
EOC

service postfix restart
