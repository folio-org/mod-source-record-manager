package org.folio.services;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.dao.MappingRuleDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Optional;

@Service
public class MappingRuleServiceImpl implements MappingRuleService {
  private static final Logger LOGGER = LoggerFactory.getLogger(MappingRuleServiceImpl.class);
  private static final Charset DEFAULT_RULES_ENCODING = Charsets.UTF_8;
  private static final String DEFAULT_RULES_PATH = "templates/db_scripts/rules/rules.json";
  private MappingRuleDao mappingRuleDao;

  public MappingRuleServiceImpl(@Autowired MappingRuleDao mappingRuleDao) {
    this.mappingRuleDao = mappingRuleDao;
  }

  @Override
  public Future<Optional<JsonObject>> get(String tenantId) {
    return mappingRuleDao.get(tenantId);
  }

  @Override
  public Future<Void> saveDefaultRules(String tenantId) {
    Future<Void> future = Future.future();
    Optional<String> optionalRules = readResourceFromPath(DEFAULT_RULES_PATH);
    if (optionalRules.isPresent()) {
      String rules = optionalRules.get();
      if (isValidJson(rules)) {
        mappingRuleDao.save(new JsonObject(rules), tenantId).setHandler(ar -> {
          if (ar.failed()) {
            LOGGER.error("Can not save rules for tenant {}", tenantId, ar.cause());
            future.fail(ar.cause());
          } else {
            future.complete();
          }
        });
      } else {
        String errorMessage = "Can not save rules in non-JSON format";
        LOGGER.error(errorMessage);
        future.fail(errorMessage);
      }
    } else {
      String errorMessage = "No default rules found";
      LOGGER.error(errorMessage);
      future.fail(new InternalServerErrorException(errorMessage));
    }
    return future;
  }

  @Override
  public Future<JsonObject> update(String rules, String tenantId) {
    Future<JsonObject> future = Future.future();
    if (isValidJson(rules)) {
      mappingRuleDao.update(new JsonObject(rules), tenantId).setHandler(future);
    } else {
      String errorMessage = "Can not update rules in non-JSON format";
      LOGGER.error(errorMessage);
      future.fail(new BadRequestException(errorMessage));
    }
    return future;
  }

  @Override
  public Future<JsonObject> restore(String tenantId) {
    Future<JsonObject> future = Future.future();
    Optional<String> optionalRules = readResourceFromPath(DEFAULT_RULES_PATH);
    if (optionalRules.isPresent()) {
      String rules = optionalRules.get();
      update(rules, tenantId).setHandler(future);
    } else {
      String errorMessage = "No rules found in resources";
      LOGGER.error(errorMessage);
      future.fail(new InternalServerErrorException(errorMessage));
    }
    return future;
  }

  /**
   * Returns true if given String is valid JSON
   *
   * @param json given string
   * @return true if given String is valid JSON, false if non-valid
   */
  private boolean isValidJson(String json) {
    try {
      new JsonObject(json);
      return true;
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
      return false;
    }
  }

  /**
   * Returns Optional with found resource
   *
   * @param path path to file from resources
   * @return optional with resource, empty if no file in resources
   */
  private Optional<String> readResourceFromPath(String path) {
    URL url = Resources.getResource(path);
    try {
      return Optional.of(Resources.toString(url, DEFAULT_RULES_ENCODING));
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      return Optional.empty();
    }
  }
}
