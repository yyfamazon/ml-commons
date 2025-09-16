package org.opensearch.ml.jobs.processors;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.commons.InjectSecurity;
import org.opensearch.commons.authuser.User;
import java.io.Closeable;
import java.util.List;
import java.util.function.Supplier;

@Log4j2
public class InjectorContextElement implements Closeable {

    @Getter
    private final List<String> roles;
    private final User user;
    private final InjectSecurity rolesInjectorHelper;

    public InjectorContextElement(
            String id,
            Settings settings,
            ThreadContext threadContext,
            List<String> roles,
            User user
    ) {
        this.roles = roles;
        this.user = user;
        this.rolesInjectorHelper = new InjectSecurity(id, settings, threadContext);
        log.info("roles size is" + roles.size());
        rolesInjectorHelper.injectRoles(roles);
        rolesInjectorHelper.injectUser("admin");
        log.info("roles is" + roles);
    }

    public InjectorContextElement(
            String id,
            Settings settings,
            ThreadContext threadContext,
            List<String> roles
    ) {
        this(id, settings, threadContext, roles, null);
    }

    public <T> T withClosableContext(Supplier<T> block) {
//        close();
        return block.get();
    }

    @Override
    public void close() {
        log.info("closed is");
        rolesInjectorHelper.close();
    }
}
