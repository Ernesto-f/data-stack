<#import "footer.ftl" as loginFooter>
<#import "theme-resources.ftl" as themeResourceTags>

<#macro registrationLayout bodyClass="" displayInfo=false displayMessage=true displayRequiredFields=false>
<!DOCTYPE html>

<html
    class="${properties.kcHtmlClass!}"
    lang="${lang}"
    <#if realm.internationalizationEnabled>
        dir="${(locale.rtl)?then('rtl','ltr')}"
    </#if>
>
<head>

    <link
      rel="icon"
      type="image/png"
      href="https://portal.izatacore.com/pulse-fav32.png"
    />

    <meta charset="utf-8">
    <meta
        http-equiv="Content-Type"
        content="text/html; charset=UTF-8"
    />

    <#if properties.meta?has_content>
        <#list properties.meta?split(' ') as meta>
            <meta
                name="${meta?split('==')[0]}"
                content="${meta?split('==')[1]}"
            />
        </#list>
    </#if>

    <title>${title!}</title>

    <#if themeResources?? && themeResources.favicons?has_content>

        <@themeResourceTags.renderFavicons
            themeResources.favicons
            url.resourcesPath
        />

    <#else>

        <link
            rel="icon"
            href="${url.resourcesPath}/img/favicon.ico"
        />

    </#if>


    <#if themeResources?? && themeResources.stylesCommon?has_content>

        <@themeResourceTags.renderStyles
            themeResources.stylesCommon
            url.resourcesCommonPath
        />

    <#elseif properties.stylesCommon?has_content>

        <#list properties.stylesCommon?split(' ') as style>

            <link
                href="${url.resourcesCommonPath}/${style}"
                rel="stylesheet"
            />

        </#list>

    </#if>


    <#if themeResources?? && themeResources.styles?has_content>

        <@themeResourceTags.renderStyles
            themeResources.styles
            url.resourcesPath
        />

    <#elseif properties.styles?has_content>

        <#list properties.styles?split(' ') as style>

            <link
                href="${url.resourcesPath}/${style}"
                rel="stylesheet"
            />

        </#list>

    </#if>


    <#if themeResources?? && themeResources.scripts?has_content>

        <@themeResourceTags.renderScripts
            themeResources.scripts
            url.resourcesPath
            "text/javascript"
        />

    <#elseif properties.scripts?has_content>

        <#list properties.scripts?split(' ') as script>

            <script
                src="${url.resourcesPath}/${script}"
                type="text/javascript">
            </script>

        </#list>

    </#if>


    <script type="importmap">
        {
            "imports": {
                "rfc4648": "${url.resourcesCommonPath}/vendor/rfc4648/rfc4648.js"
            }
        }
    </script>


    <script
        src="${url.resourcesPath}/js/menu-button-links.js"
        type="module">
    </script>


    <#if scripts??>

        <#list scripts as script>

            <script
                src="${script}"
                type="text/javascript">
            </script>

        </#list>

    </#if>


    <script type="module">

        <#outputformat "JavaScript">

        import { startSessionPolling }
        from ${(url.resourcesPath + "/js/authChecker.js")?c};

        startSessionPolling(
            ${url.ssoLoginInOtherTabsUrl?c}
        );

        </#outputformat>

    </script>


    <#if authenticationSession??>

        <script type="module">

            <#outputformat "JavaScript">

            import { checkAuthSession }
            from ${(url.resourcesPath + "/js/authChecker.js")?c};

            checkAuthSession(
                ${authenticationSession.authSessionIdHash?c}
            );

            </#outputformat>

        </script>

    </#if>

</head>

<body class="${properties.kcBodyClass!} ${bodyClass}">

<div class="pulse-auth-page">


    <!-- ====================================================== -->
    <!-- LADO INSTITUCIONAL                                    -->
    <!-- ====================================================== -->

    <aside
        class="pulse-brand-panel"
        aria-label="Pulse Portal"
    >

        <div class="pulse-brand-glow pulse-brand-glow--blue"></div>
        <div class="pulse-brand-glow pulse-brand-glow--purple"></div>
        <div class="pulse-brand-glow pulse-brand-glow--green"></div>


        <!-- Símbolo decorativo de fundo -->

        <img
            class="pulse-brand-symbol"
            src="${properties.pulseSymbol!'https://portal.izatacore.com/pulse-icon.svg'}"
            alt=""
            aria-hidden="true"
        />


        <div class="pulse-brand-content">


            <!-- LOGO -->

            <div class="pulse-brand-top">

                <img
                    class="pulse-brand-logo"
                    src="${properties.pulseLogo!'https://portal.izatacore.com/pulse-logo.svg'}"
                    alt="Pulse"
                />

            </div>


            <!-- TEXTO INSTITUCIONAL -->

            <div class="pulse-brand-copy">


                <div class="pulse-eyebrow">

                    <span class="pulse-eyebrow-dot"></span>

                    BAT · Data Driven

                </div>


                    <h1 class="pulse-main-message">

                        <span
                            id="pulse-typewriter"
                            class="pulse-typewriter"
                            aria-live="polite"
                        ></span>

                        <span
                            class="pulse-typewriter-cursor"
                            aria-hidden="true"
                        ></span>

                    </h1>


                <p>

                    ${msg("pulseBrandDescription")}

                </p>


            </div>

        </div>

    </aside>



    <!-- ====================================================== -->
    <!-- LOGIN                                                  -->
    <!-- ====================================================== -->

    <main class="pulse-auth-main">


        <section
            class="pulse-auth-card"
            aria-labelledby="kc-page-title"
        >


            <!-- LOGO MOBILE -->

            <div class="pulse-mobile-brand">

                <img
                    src="${properties.pulseLogo!'https://portal.izatacore.com/pulse-logo.svg'}"
                    alt="Pulse"
                />

            </div>



            <!-- ================================================== -->
            <!-- IDIOMA                                             -->
            <!-- ================================================== -->

            <#if
                realm.internationalizationEnabled
                &&
                locale.supported?size gt 1
            >

                <div
                    class="pulse-locale"
                    id="kc-locale"
                >

                    <div id="kc-locale-wrapper">

                        <div
                            id="kc-locale-dropdown"
                            class="menu-button-links ${properties.kcLocaleDropDownClass!}"
                        >

                            <button

                                tabindex="1"

                                id="kc-current-locale-link"

                                aria-label="${msg("languages")}"

                                aria-haspopup="true"

                                aria-expanded="false"

                                aria-controls="language-switch1"
                            >

                                ${locale.current}

                            </button>


                            <ul

                                role="menu"

                                tabindex="-1"

                                aria-labelledby="kc-current-locale-link"

                                id="language-switch1"

                                class="${properties.kcLocaleListClass!}"
                            >

                                <#assign i = 1>


                                <#list locale.supported as l>

                                    <li
                                        class="${properties.kcLocaleListItemClass!}"
                                        role="none"
                                    >

                                        <a

                                            role="menuitem"

                                            id="language-${i}"

                                            class="${properties.kcLocaleItemClass!}"

                                            href="${l.url}"
                                        >

                                            ${l.label}

                                        </a>

                                    </li>


                                    <#assign i++>


                                </#list>

                            </ul>

                        </div>

                    </div>

                </div>

            </#if>



            <!-- ================================================== -->
            <!-- CABEÇALHO                                          -->
            <!-- ================================================== -->

            <header class="pulse-card-header">


                <div class="pulse-card-kicker">

                    PULSE PORTAL

                </div>


                <#if
                    !(
                        auth?has_content
                        &&
                        auth.showUsername()
                        &&
                        !auth.showResetCredentials()
                    )
                >


                    <#if displayRequiredFields>


                        <div class="pulse-title-row">


                            <h2 id="kc-page-title">

                                <#nested "header">

                            </h2>


                            <span class="pulse-required">

                                <span>*</span>

                                ${msg("requiredFields")}

                            </span>


                        </div>


                    <#else>


                        <h2 id="kc-page-title">

                            <#nested "header">

                        </h2>


                    </#if>


                <#else>


                    <#nested "show-username">


                    <div
                        id="kc-username"
                        class="pulse-current-user"
                    >


                        <span id="kc-attempted-username">

                            ${auth.attemptedUsername}

                        </span>


                        <a
                            id="reset-login"
                            href="${url.loginRestartFlowUrl}"
                            aria-label="${msg("restartLoginTooltip")}"
                        >

                            ${msg("restartLoginTooltip")}

                        </a>


                    </div>


                </#if>


                <p class="pulse-card-subtitle">

                    ${msg("pulseAuthSubtitle")}

                </p>


            </header>



            <!-- ================================================== -->
            <!-- CONTEÚDO KEYCLOAK                                  -->
            <!-- ================================================== -->

            <div
                id="kc-content"
                class="pulse-card-content"
            >


                <div id="kc-content-wrapper">



                    <!-- ========================================== -->
                    <!-- MENSAGENS                                  -->
                    <!-- ========================================== -->

                    <#if
                        displayMessage
                        &&
                        message?has_content
                        &&
                        (
                            message.type != 'warning'
                            ||
                            !isAppInitiatedAction??
                        )
                    >


                        <div

                            class="pulse-alert pulse-alert--${message.type}"

                            role="alert"
                        >


                            <span
                                class="pulse-alert-icon"
                                aria-hidden="true"
                            ></span>


                            <span>

                                ${kcSanitize(message.summary)?no_esc}

                            </span>


                        </div>


                    </#if>



                    <!-- ========================================== -->
                    <!-- FORMULÁRIO KEYCLOAK                        -->
                    <!-- ========================================== -->

                    <#nested "form">



                    <!-- ========================================== -->
                    <!-- OUTRA FORMA DE LOGIN                       -->
                    <!-- ========================================== -->

                    <#if
                        auth?has_content
                        &&
                        auth.showTryAnotherWayLink()
                    >


                        <form

                            id="kc-select-try-another-way-form"

                            action="${url.loginAction}"

                            method="post"
                        >


                            <input
                                type="hidden"
                                name="tryAnotherWay"
                                value="on"
                            />


                            <a

                                href="#"

                                id="try-another-way"

                                class="pulse-secondary-action"

                                onclick="
                                    document.forms[
                                        'kc-select-try-another-way-form'
                                    ].requestSubmit();

                                    return false;
                                "
                            >

                                ${msg("doTryAnotherWay")}

                            </a>


                        </form>


                    </#if>



                    <!-- ========================================== -->
                    <!-- TROCA DE ORGANIZAÇÃO                       -->
                    <!-- ========================================== -->

                    <#if
                        switchOrganizationEnabled??
                        &&
                        switchOrganizationEnabled
                    >


                        <form

                            id="kc-switch-organization-form"

                            action="${url.loginAction}"

                            method="post"
                        >


                            <input
                                type="hidden"
                                name="switchOrganization"
                                value="true"
                            />


                            <a

                                href="#"

                                id="switch-organization"

                                class="pulse-secondary-action"

                                onclick="
                                    document.forms[
                                        'kc-switch-organization-form'
                                    ].requestSubmit();

                                    return false;
                                "
                            >

                                ${msg("doSwitchOrganization")}

                            </a>


                        </form>


                    </#if>



                    <!-- ========================================== -->
                    <!-- PROVIDERS SOCIAIS                          -->
                    <!-- ========================================== -->

                    <#nested "socialProviders">



                    <!-- ========================================== -->
                    <!-- INFORMAÇÕES                                -->
                    <!-- ========================================== -->

                    <#if displayInfo>


                        <div
                            id="kc-info"
                            class="pulse-info"
                        >


                            <div id="kc-info-wrapper">

                                <#nested "info">

                            </div>


                        </div>


                    </#if>


                </div>

            </div>



            <!-- ================================================== -->
            <!-- SEGURANÇA                                          -->
            <!-- ================================================== -->

            <div class="pulse-card-security">

                <span
                    class="pulse-lock"
                    aria-hidden="true"
                ></span>

                ${msg("pulseSecurityNotice")}

            </div>



            <!-- ================================================== -->
            <!-- FOOTER KEYCLOAK                                    -->
            <!-- ================================================== -->

            <@loginFooter.content/>


        </section>



        <div class="pulse-page-footer">

            Pulse Portal

            <span>•</span>

            BAT - Data Driven

        </div>


    </main>


</div>


</body>

</html>

</#macro>
