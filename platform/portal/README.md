# Pulse Portal

Catálogo de acesso às aplicações demo/dev da IzataCore. A página é servida por Nginx em `https://portal.izatacore.com`, protegida pelo Keycloak/OAuth2 Proxy no grupo `/pulse/home`; os cards abrem as aplicações nos respectivos subdomínios, que mantêm sua própria autorização.

## Serviços listados

- Samia — `knowledge.izatacore.com`
- Pulse Analytics — `analytics.izatacore.com`
- Pulse Automation — `automation.izatacore.com`
- Pulse DataLogger — `datalogger.izatacore.com`
- Pulse SmartETL — `smartetl.izatacore.com`
- Pulse Archive — `archive.izatacore.com`

Esta versão enxuta do **Pulse Portal** usa links para os subdomínios atuais. Os produtos integrados usam identidades individuais do Keycloak e grupos de acesso específicos por serviço.
