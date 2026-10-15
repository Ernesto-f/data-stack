FROM alpine:3.21

RUN apk add --no-cache \
    nginx \
    nginx-mod-http-headers-more \
    tzdata \
    apache2-utils

# criar diretório padrão para auth
RUN mkdir -p /etc/nginx/auth

CMD ["nginx", "-g", "daemon off;"]