package org.folio.config;

import io.vertx.core.Vertx;
import org.folio.services.journal.JournalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.services"})
public class ApplicationConfig {

  @Autowired
  private Vertx vertx;

  @Bean(value = "journalServiceProxy")
  public JournalService journalServiceProxy() {
    return JournalService.createProxy(vertx);
  }
}
