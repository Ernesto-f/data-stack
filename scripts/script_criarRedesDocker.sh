#!/usr/bin/env sh


#para rodar execute como adm
# em /data-stack/infra/
#chmod +x script_criarRedesDocker.sh
#./script_criarRedesDocker.sh
set -eu

create_network() {
    network_name="$1"

    if docker network inspect "$network_name" >/dev/null 2>&1; then
        echo "Rede já existe: $network_name"
    else
        echo "Criando rede: $network_name"
        docker network create \
            --driver bridge \
            "$network_name"
    fi
}

create_network "infra-auth-edge"
create_network "infra-apextools-edge"
create_network "infra-datastack-edge"
create_network "infra-apiapps-edge"