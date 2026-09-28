#!/bin/sh
set -eu
cd "$(dirname "$0")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/ca.key" -out "$work/ca.pem" -days 36500 -subj '/CN=tokio-tcp-pool test CA' -addext 'basicConstraints=critical,CA:TRUE'
openssl req -new -newkey rsa:2048 -nodes -keyout "$work/server.key" -out "$work/server.csr" -subj '/CN=localhost'
cat > "$work/extensions" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost
EXT
openssl x509 -req -in "$work/server.csr" -CA "$work/ca.pem" -CAkey "$work/ca.key" -CAcreateserial -out "$work/server.pem" -days 36500 -extfile "$work/extensions"
openssl x509 -in "$work/ca.pem" -outform DER -out ca.der
openssl x509 -in "$work/server.pem" -outform DER -out server.der
openssl pkcs8 -topk8 -nocrypt -in "$work/server.key" -outform DER -out server-key.der
