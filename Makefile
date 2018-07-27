# Copyright 2018 Comcast Cable Communications Management, LLC
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

pulsarSrc := ${GOPATH}/src/github.com/apache/incubator-pulsar

certDir := certs
certDirAbs := $(shell pwd)/${certDir}
certAppRole := app
certAdminRole := admin

# Generate the go protobuffer file(s) using Pulsar's protobuffer definitions.
# It manually sets the generated package's name to "api" to match the Java Pulsar
# library's naming convention.
#
# Requirements:
#  * protoc and protoc-gen-go are installed. See: https://github.com/golang/protobuf
#  * The Pulsar project checked out at $GOPATH/src/github.com/apache/incubator-pulsar
api/PulsarApi.pb.go:
	cd api && ./generate.bash ${pulsarSrc}

# When running the standalone server using TLS, the sample
# topics aren't created properly. This target performs the
# necessary admin commands to create the topic and permissions.
.PHONY: standalone-tls-ns
standalone-tls-ns:
	${pulsarSrc}/bin/pulsar-admin \
		tenants create sample \
		--admin-roles admin \
		--allowed-clusters standalone || true
	@sleep 1
	${pulsarSrc}/bin/pulsar-admin \
		namespaces create sample/standalone/ns1 || true
	@sleep 1
	${pulsarSrc}/bin/pulsar-admin \
		namespaces grant-permission sample/standalone/ns1 \
		--actions produce,consume \
		--role ${certAppRole} || true

.PHONY: pulsar-tls-conf
pulsar-tls-conf: pulsar-conf/client.tls.conf pulsar-conf/standalone.tls.conf

# Use the client.conf template to create a client.conf
# set to use the certificates in the ${certDir}.
pulsar-conf/client.tls.conf: pulsar-conf/client.tls.conf.tmpl
	sed \
		-e 's#$$CERT_DIR#${certDirAbs}#g' pulsar-conf/client.tls.conf.tmpl \
		> pulsar-conf/client.tls.conf

# Use the standalone.conf template to create a standalone.conf
# set to use the certificates in the ${certDir}.
pulsar-conf/standalone.tls.conf: pulsar-conf/standalone.tls.conf.tmpl
	sed \
		-e 's#$$CERT_DIR#${certDirAbs}#g' pulsar-conf/standalone.tls.conf.tmpl \
		> pulsar-conf/standalone.tls.conf

###################
## TLS certificates
###################

# The certificates target will generate a root CA, then create
# broker, admin, and application certs from it. These can be
# used for running the standalone server with TLS enabled.
# The will all be created in ${certDir}.
.PHONY: certificates
certificates: ${certDir}/rootCA.crt ${certDir}/broker.pem ${certDir}/admin.pem ${certDir}/app.pem

${certDir}:
	mkdir -p ${certDir}

##
## Root CA
##

${certDir}/rootCA.key: ${certDir}
	openssl genrsa \
		-des3 \
		-out $@ \
		4096

${certDir}/rootCA.crt: ${certDir} ${certDir}/rootCA.key
	openssl req \
		-x509 \
		-new \
		-nodes \
		-key ${certDir}/rootCA.key \
		-subj '/CN=pulsar-local-tls-dev' \
		-sha256 \
		-days 1024 \
		-out $@

##
## Broker certs
##

${certDir}/broker.key.pem: ${certDir}
	openssl genrsa \
		-out ${certDir}/broker.key \
		2048
	openssl pkcs8 \
		-topk8 \
		-nocrypt \
		-inform PEM \
		-outform PEM \
		-in ${certDir}/broker.key \
		-out $@

${certDir}/broker.csr: ${certDir}/broker.key.pem
	openssl req \
		-newkey rsa:2048 \
		-key ${certDir}/broker.key \
		-sha256 \
		-subj '/CN=localhost' \
		-nodes \
		-in ${certDir}/rootCA.key \
		-out $@ \
		-outform PEM

${certDir}/broker.pem: ${certDir}/broker.csr
	openssl x509 \
		-req \
		-in ${certDir}/broker.csr \
		-CA ${certDir}/rootCA.crt \
		-CAkey ${certDir}/rootCA.key \
		-CAcreateserial \
		-out $@ \
		-days 500 \
		-sha256

##
## Admin certs
##

${certDir}/admin.key.pem: ${certDir}
	openssl genrsa \
		-out ${certDir}/admin.key \
		2048
	openssl pkcs8 \
		-topk8 \
		-nocrypt \
		-inform PEM \
		-outform PEM \
		-in ${certDir}/admin.key \
		-out $@

${certDir}/admin.csr: ${certDir}/admin.key.pem
	openssl req \
		-newkey rsa:2048 \
		-key ${certDir}/admin.key \
		-sha256 \
		-subj '/CN=${certAdminRole}' \
		-nodes \
		-out $@ \
		-outform PEM

${certDir}/admin.pem: ${certDir}/admin.csr
	openssl x509 \
		-req \
		-in ${certDir}/admin.csr \
		-CA ${certDir}/rootCA.crt \
		-CAkey ${certDir}/rootCA.key \
		-CAcreateserial \
		-out $@ \
		-days 500 \
		-sha256

##
## App certs
##

${certDir}/app.key.pem: ${certDir}
	openssl genrsa \
		-out ${certDir}/app.key \
		2048
	openssl pkcs8 \
		-topk8 \
		-nocrypt \
		-inform PEM \
		-outform PEM \
		-in ${certDir}/app.key \
		-out $@

${certDir}/app.csr: ${certDir}/app.key.pem
	openssl req \
		-newkey rsa:2048 \
		-key ${certDir}/app.key \
		-sha256 \
		-subj "/CN=${certAppRole}" \
		-nodes \
		-out $@ \
		-outform PEM

${certDir}/app.pem: ${certDir}/app.csr
	openssl x509 \
		-req \
		-in ${certDir}/app.csr \
		-CA ${certDir}/rootCA.crt \
		-CAkey ${certDir}/rootCA.key \
		-CAcreateserial \
		-out $@ \
		-days 500 \
		-sha256

