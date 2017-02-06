package com.joint;

import org.openrdf.repository.Repository;
import virtuoso.sesame2.driver.VirtuosoRepository;
import wwwc.nees.joint.module.kao.RepositoryConfig;

/**
 *
 * @author armando
 */
public class VirtuosoPersistence implements RepositoryConfig {

    private final String virtuosoServer = "jdbc:virtuoso://localhost:1111";
    private final String user = "dba";
    private final String password = "dba";

    /**
     * Connect to graph in the Virtuoso repository
     *
     * @return Repository the Virtuoso Repository connected
     */
    @Override
    public Repository createNewRepository() {
        return new VirtuosoRepository(virtuosoServer, user, password);
    }

}
