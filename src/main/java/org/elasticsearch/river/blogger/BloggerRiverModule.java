package org.elasticsearch.river.blogger;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

/**
 * @author Ningning Xia
 */
public class BloggerRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(BloggerRiver.class).asEagerSingleton();
    }
}
