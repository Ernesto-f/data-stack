SEPARAÇÃO DA ARQUITETURA

apextools
data-stack/infra/compose.apextools.yml
    dbeaver cloud
    guacamole

auth
data-stack/infra/compose.auth.yml
    keykloak
    nginx
    oauth2

apps
data-stack/apps/infra/
    fastapi
    flutter


REDES EXTERNAS
"infra-auth-edge"
"infra-apextools-edge"
"infra-datastack-edge"
"infra-apiapps-edge"

para criar / adicionar, acesse ->
    script_criarRedesDocker.sh
