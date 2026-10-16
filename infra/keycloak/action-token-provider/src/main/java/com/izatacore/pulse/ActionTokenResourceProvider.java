package com.izatacore.pulse;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.ext.Provider;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.keycloak.authentication.actiontoken.execactions.ExecuteActionsActionToken;
import org.keycloak.common.util.Time;
import org.keycloak.models.ClientModel;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.UserModel;
import org.keycloak.models.utils.SystemClientUtil;
import org.keycloak.services.managers.AppAuthManager;
import org.keycloak.services.resource.RealmResourceProvider;
import org.keycloak.services.Urls;

@Provider
@Path("")
public final class ActionTokenResourceProvider implements RealmResourceProvider {
    private static final List<String> DEFAULT_ACTIONS = List.of("VERIFY_EMAIL", "UPDATE_PASSWORD");
    private static final int INVITATION_LIFESPAN_SECONDS = 86_400;

    private final KeycloakSession session;

    ActionTokenResourceProvider(KeycloakSession session) {
        this.session = session;
    }

    @Override
    public Object getResource() {
        return this;
    }

    @Override
    public void close() {
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response createInvitationLink(
            @QueryParam("userId") String userId,
            @QueryParam("lifespan") Integer lifespan) {
        AppAuthManager.AuthResult auth = new AppAuthManager.BearerTokenAuthenticator(session).authenticate();
        if (auth == null || auth.getToken() == null) {
            throw new NotAuthorizedException("Bearer");
        }

        String expectedClient = System.getenv().getOrDefault(
                "PULSE_ACTION_TOKEN_CLIENT_ID", "pulse-tenant-control");
        if (!expectedClient.equals(auth.getToken().getIssuedFor())) {
            throw new NotAuthorizedException("Bearer");
        }

        RealmModel realm = session.getContext().getRealm();
        UserModel user = session.users().getUserById(realm, userId);
        if (user == null || !user.isEnabled() || user.getEmail() == null) {
            return Response.status(Response.Status.NOT_FOUND).build();
        }

        ClientModel client = SystemClientUtil.getSystemClient(realm);
        if (client == null || !client.isEnabled()) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
        }

        int validFor = lifespan == null ? INVITATION_LIFESPAN_SECONDS : lifespan;
        if (validFor < 60 || validFor > 604_800) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "lifespan must be between 60 and 604800 seconds"))
                    .build();
        }

        int expiration = Time.currentTime() + validFor;
        ExecuteActionsActionToken token = new ExecuteActionsActionToken(
                user.getId(),
                user.getEmail(),
                expiration,
                DEFAULT_ACTIONS,
                null,
                client.getClientId());

        UriBuilder builder = Urls.actionTokenBuilder(
                session.getContext().getUri().getBaseUri(),
                token.serialize(session, realm, session.getContext().getUri()),
                client.getClientId(),
                null,
                null);
        String link = builder.build(realm.getName()).toString();

        return Response.ok(Map.of(
                        "link", link,
                        "expires_at", expiration,
                        "actions", Arrays.asList("VERIFY_EMAIL", "UPDATE_PASSWORD")))
                .type(MediaType.APPLICATION_JSON)
                .header("Cache-Control", "no-store")
                .build();
    }
}
