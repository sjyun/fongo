package com.github.fakemongo.integration;

import org.springframework.data.mongodb.repository.MongoRepository;

/**
 * @author Tom Dearman
 */
public interface SpringModelMapRepository extends MongoRepository<SpringModelMap, String> {
}
