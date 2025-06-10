#!/usr/bin/env bash

sed -i '/rgw frontends =/a\
rgw keystone api version = 3\
rgw keystone url = http://{{.AuthHost}}:{{.ExternalAuthPort}}\
rgw keystone accepted roles = {{.OperatorRole}},{{.ResellerRole}}\
rgw keystone admin user = {{.AdminUser}}\
rgw keystone admin password = {{.AdminPassword}}\
rgw keystone admin project = {{.AdminProject}}\
rgw keystone admin domain = {{.AdminDomain}}\
rgw keystone implicit tenants = false\
rgw keystone verify ssl = false\
rgw s3 auth use keystone = true\
rgw swift account in url = true\
rgw swift url prefix = swift' /opt/ceph-container/bin/demo  

exec "$@"