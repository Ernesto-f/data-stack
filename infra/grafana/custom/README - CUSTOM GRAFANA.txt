PARA MUDAR O TEXTO NO LOGIN 

e02828fa2ed0:/usr/share/grafana$ find /usr/share/grafana/public/build/ -name *.js -exec sed -i 's|LoginTitle="Welcome to Grafana"|LoginTitle="Sua Nova Mensagem"|g' {} +

NA PASTA CSS ESTÃO OS MODS DA PAGINA DE LOGIN, PRA NAO MOSTRAR MAIS ALGUMAS INFOS.
 -> tira welcome grafana
 -> tira rodapé

NA PASTA IMG, EXISTEM AS BINDS COM OS VOLUMES DO DOCKER GRAFANA (VER LA NO DOCKER COMPOSE)
    -> Essas imagems forçam a troca para as imagens que ecolhemos.


NO NGINX tambem existem intercepts para trocar alguns textos.
Segue o location configurado para caso precise fazer em outro servidor. 

location /analytics/ {
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /analytics;
    proxy_set_header Accept-Encoding "";

    proxy_pass http://grafana:3000;

    # Branding + JS injection
    sub_filter_types text/html;
    sub_filter_once off;

    # title inicial (antes do JS carregar)
    sub_filter '<title>Grafana</title>' '<title>Apex Analytics</title>';

    # Ativa filtro de substituição
    sub_filter 'Welcome to Grafana' 'Apex Analytics';

    # injeta 1 script que faz: title + texto do header
    sub_filter '</head>' '<script>
    (function () {
    var desired = "Apex Analytics";

    function apply() {
        // title
        if (document.title !== desired) document.title = desired;

        // texto do header (classe atual do Grafana 12)
        var el = document.querySelector("span.css-uantyg");
        if (el && el.textContent.trim() === "Grafana") el.textContent = desired;
    }

    apply();

    // SPA re-render
    try { new MutationObserver(apply).observe(document.documentElement, {subtree:true, childList:true}); } catch(e) {}

    // fallback
    setInterval(apply, 800);

    // navegação interna
    var _push = history.pushState;
    history.pushState = function(){ _push.apply(this, arguments); setTimeout(apply, 0); };
    window.addEventListener("popstate", function(){ setTimeout(apply, 0); });
    })();
    </script></head>';

    }

    #Proxy Grafana Live WebSocket connections.
    location /api/live/ {
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection $connection_upgrade;
        proxy_set_header Host $host;
        proxy_pass http://grafana:3000;
        proxy_set_header Accept-Encoding "";

    }
