#!/bin/bash     
  
ca_path=ca  
certs_path=$ca_path/certs  
newcerts_path=$ca_path/newcerts  
private_path=$ca_path/private  
crl_path=$ca_path/crl  
  
echo 移除CA根目录     
rm -rf ca     
    
echo 构建CA根目录     
mkdir ca     
   
echo 构建子目录     
mkdir certs     
mkdir newcerts     
mkdir private     
mkdir crl    
  
#构建文件     
touch $ca_path/index.txt  
echo 01 > $ca_path/serial  
echo      
  
#构建随机数     
openssl rand -out $private_path/.rand 1000  
echo      
  
echo 生成根证书私钥     
openssl genrsa -des3 -out $private_path/ca.pem 2048  
echo     
  
echo 查看私钥信息  
openssl rsa -noout -text -in $private_path/ca.pem  
echo  
  
echo 生成根证书请求      
openssl req -new -key $private_path/ca.pem -out $certs_path/ca.csr -subj "/C=CN/ST=BJ/L=BJ/O=zlex/OU=zlex/CN=ca.zlex.org"  
echo     
  
echo 查看证书请求   
openssl req -in $certs_path/ca.csr -text -noout  
echo   
  
echo 签发根证书     
openssl ca -create_serial -out $certs_path/ca.crt -days 3650 -batch -keyfile $private_path/ca.pem -selfsign -extensions v3_ca -infiles $certs_path/ca.csr   
#openssl x509 -req -sha1 -extensions v3_ca -signkey $private_path/ca.pem -in $certs_path/ca.csr -out $certs_path/ca.crt -days 3650  
echo     
  
echo 查看证书详情  
openssl x509 -in $certs_path/ca.crt -text -noout  
echo  
  
echo 证书转换——根证书     
openssl pkcs12 -export -clcerts -in $certs_path/ca.crt -inkey $private_path/ca.pem -out $certs_path/ca.p12  
echo    
  
echo 生成服务器端私钥     
openssl genrsa -des3 -out $private_path/server.pem 1024  
echo     
  
echo 查看私钥信息  
openssl rsa -noout -text -in $private_path/server.pem  
echo  
  
echo 生成服务器端证书请求     
openssl req -new -key $private_path/server.pem -out $certs_path/server.csr -subj "/C=CN/ST=BJ/L=BJ/O=zlex/OU=zlex/CN=www.zlex.org"  
echo     
  
echo 查看证书请求   
openssl req -in $certs_path/server.csr -text -noout  
echo   
  
echo 签发服务器端证书  
openssl ca -in $certs_path/server.csr -out $certs_path/server.crt -cert $certs_path/ca.crt -keyfile $private_path/ca.pem -days 365 -notext  
#openssl x509 -req -days 365 -sha1 -extensions v3_req -CA $certs_path/ca.crt -CAkey $private_path/ca.pem -CAserial $ca_path/serial -CAcreateserial -in $certs_path/server.csr -out $certs_path/server.crt  
echo  
  
echo 查看证书详情  
openssl x509 -in $certs_path/server.crt -text -noout  
echo  
  
echo 证书转换——服务器端     
openssl pkcs12 -export -clcerts -in $certs_path/server.crt -inkey $private_path/server.pem -out $certs_path/server.p12  
echo      
  
echo 生成客户端私钥     
openssl genrsa -des3 -out $private_path/client.pem 1024  
echo     
  
echo 生成客户端私钥     
openssl genrsa -des3 -out $private_path/client.pem 1024  
echo     
  
echo 查看私钥信息  
openssl rsa -noout -text -in $private_path/client.pem  
echo  
  
echo 生成客户端证书请求     
openssl req -new -key $private_path/client.pem -out $certs_path/client.csr -subj "/C=CN/ST=BJ/L=BJ/O=zlex/OU=zlex/CN=zlex"  
echo     
  
echo 查看证书请求   
openssl req -in $certs_path/client.csr -text -noout  
echo   
  
echo 签发客户端证书     
openssl ca -in $certs_path/client.csr -out $certs_path/client.crt -cert $certs_path/ca.crt -keyfile $private_path/ca.pem -days 365 -notext  
#openssl x509 -req -days 365 -sha1 -extensions dir_sect -CA $certs_path/ca.crt -CAkey $private_path/ca.pem -CAserial $ca_path/serial -in $certs_path/client.csr -out $certs_path/client.crt  
echo     
  
echo 查看证书详情  
openssl x509 -in $certs_path/client.crt -text -noout  
echo  
  
echo 证书转换——客户端     
openssl pkcs12 -export -clcerts -in $certs_path/client.crt -inkey $private_path/client.pem -out $certs_path/client.p12  
echo   
  
echo 生成证书链PKCS#7  
openssl crl2pkcs7 -nocrl -certfile $certs_path/server.crt -certfile $certs_path/ca.crt -certfile $certs_path/client.crt -out  
form PEM -out $certs_path/zlex.p7b  
echo  
  
echo 查看证书链  
openssl pkcs7 -in $certs_path/zlex.p7b -print_certs -noout  
