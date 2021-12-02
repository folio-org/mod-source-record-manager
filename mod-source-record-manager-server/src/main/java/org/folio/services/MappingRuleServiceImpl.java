package org.folio.services;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Function;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;

import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import org.folio.Record;
import org.folio.dao.MappingRuleDao;
import org.folio.services.entity.MappingRuleCacheKey;

@Service
public class MappingRuleServiceImpl implements MappingRuleService {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final Charset DEFAULT_RULES_ENCODING = StandardCharsets.UTF_8;
  private static final String DEFAULT_BIB_RULES_PATH = "rules/marc_bib_rules.json";
  private static final String DEFAULT_HOLDINGS_RULES_PATH = "rules/marc_holdings_rules.json";
  private static final String DEFAULT_AUTHORITY_RULES_PATH = "rules/marc_authority_rules.json";
  private final MappingRuleDao mappingRuleDao;
  private final MappingRuleCache mappingRuleCache;

  @Autowired
  public MappingRuleServiceImpl(MappingRuleDao mappingRuleDao, MappingRuleCache mappingRuleCache) {
    this.mappingRuleDao = mappingRuleDao;
    this.mappingRuleCache = mappingRuleCache;
  }

  @Override
  public Future<Optional<JsonObject>> get(Record.RecordType recordType, String tenantId) {
    return mappingRuleDao.get(recordType, tenantId);
  }

  @Override
  public Future<Void> saveDefaultRules(Record.RecordType recordType, String tenantId) {
    Promise<Void> promise = Promise.promise();
    Optional<String> optionalRules = receiveDefaultRules(recordType);

    if (optionalRules.isPresent()) {
      String rules = optionalRules.get();
      if (isValidJson(rules)) {
        mappingRuleDao.get(recordType, tenantId)
          .compose(saveRulesIfNotExist(recordType, tenantId, rules))
          .onComplete(ar -> {
            if (ar.failed()) {
              LOGGER.error("Can not save rules for tenant {}", tenantId, ar.cause());
              promise.fail(ar.cause());
            } else {
              promise.complete();
            }
          });
      } else {
        String errorMessage = "Can not work with rules in non-JSON format";
        LOGGER.error(errorMessage);
        promise.fail(new InternalServerErrorException(errorMessage));
      }
    } else {
      String errorMessage = "No default rules found";
      LOGGER.error(errorMessage);
      promise.fail(errorMessage);
    }
    return promise.future();
  }

  @Override
  public Future<JsonObject> update(String rules, Record.RecordType recordType, String tenantId) {
    Promise<JsonObject> promise = Promise.promise();
    rejectUnsupportedType(recordType, promise);
    if (isValidJson(rules)) {
      MappingRuleCacheKey cacheKey = new MappingRuleCacheKey(tenantId, recordType);
      mappingRuleDao.update(new JsonObject(rules), recordType, tenantId)
        .onSuccess(updatedRules -> mappingRuleCache.put(cacheKey, updatedRules))
        .onComplete(promise);
    } else {
      String errorMessage = "Can not update rules in non-JSON format";
      LOGGER.error(errorMessage);
      promise.fail(new BadRequestException(errorMessage));
    }
    return promise.future();
  }

  @Override
  public Future<JsonObject> restore(Record.RecordType recordType, String tenantId) {
    Promise<JsonObject> promise = Promise.promise();
    rejectUnsupportedType(recordType, promise);
    Optional<String> optionalRules = receiveDefaultRules(recordType);

    if (optionalRules.isPresent()) {
      update(optionalRules.get(), recordType, tenantId).onComplete(promise);
    } else {
      String errorMessage = "No rules found in resources";
      LOGGER.error(errorMessage);
      promise.fail(new InternalServerErrorException(errorMessage));
    }
    return promise.future();
  }

  private Function<Optional<JsonObject>, Future<String>> saveRulesIfNotExist(Record.RecordType recordType,
                                                                             String tenantId, String defaultRules) {
    return existedRules -> {
      if (existedRules.isEmpty()) {
        return mappingRuleDao.save(new JsonObject(defaultRules), recordType, tenantId);
      } else {
        return Future.succeededFuture();
      }
    };
  }

  private Optional<String> receiveDefaultRules(Record.RecordType recordType) {
    Optional<String> optionalRules = Optional.empty();
    if (Record.RecordType.MARC_BIB == recordType) {
      optionalRules = readResourceFromPath(DEFAULT_BIB_RULES_PATH);
    } else if (Record.RecordType.MARC_HOLDING == recordType) {
      optionalRules = readResourceFromPath(DEFAULT_HOLDINGS_RULES_PATH);
    } else if (Record.RecordType.MARC_AUTHORITY == recordType) {
      optionalRules = readResourceFromPath(DEFAULT_AUTHORITY_RULES_PATH);
    }
    return optionalRules;
  }

  private void rejectUnsupportedType(Record.RecordType recordType, Promise<JsonObject> promise) {
    if (recordType == Record.RecordType.MARC_AUTHORITY) {
      String errorMessage = "Can't edit/restore MARC Authority default mapping rules";
      LOGGER.error(errorMessage);
      promise.fail(new BadRequestException(errorMessage));
    }
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
