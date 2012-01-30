package com.objectstyle.db.migration.kit;

import com.carbonfive.db.migration.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

import java.io.IOException;
import java.util.*;

import static org.springframework.util.StringUtils.collectionToCommaDelimitedString;

public class CustomResourceMigrationResolver implements MigrationResolver {
    private static final Logger log = LoggerFactory.getLogger(CustomResourceMigrationResolver.class);

    private final String migrationsLocationPattern;

    private final VersionExtractor versionExtractor;

    private final MigrationFactory migrationFactory;

    public CustomResourceMigrationResolver(String migrationsLocationPattern) {
        this.migrationsLocationPattern = migrationsLocationPattern;
        versionExtractor = new SimpleVersionExtractor();
        migrationFactory = new MigrationFactory();
    }

    public Set<Migration> resolve() {
        ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
        List<Resource> resources;
        try {
            resources = new ArrayList<Resource>(Arrays.asList(patternResolver.getResources(migrationsLocationPattern)));
        } catch (IOException e) {
            throw new MigrationException(e);
        }

        if (resources.isEmpty()) {
            String message = "No migrations were found using resource pattern '"
                    + migrationsLocationPattern + "'. Terminating migration.";
            log.error(message);
            throw new MigrationException(message);
        }

        if (log.isDebugEnabled()) {
            log.debug("Found " + resources.size() + " resources: " + collectionToCommaDelimitedString(resources));
        }

        Set<Migration> migrations = new HashSet<Migration>();
        for (Resource resource : resources) {
            String version = versionExtractor.extractVersion(resource.getFilename());
            migrations.add(migrationFactory.create(version, resource));
        }
        return migrations;
    }
}
