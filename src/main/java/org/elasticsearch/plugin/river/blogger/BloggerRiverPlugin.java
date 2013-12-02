package org.elasticsearch.plugin.river.blogger;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.blogger.BloggerRiverModule;

/**
 * @author Ningning Xia
 */

public class BloggerRiverPlugin extends AbstractPlugin {

    @Inject
    public BloggerRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-blogger";
    }

    @Override
    public String description() {
        return "River Blogger Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("blogger", BloggerRiverModule.class);
    }
}
