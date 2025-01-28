package com.github.sibdevtools.web.app.mocks;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Standalone application entry point
 *
 * @author sibmaks
 * @since 0.0.1
 */
@EnableJpaRepositories(basePackages = "com.github.sibdevtools")
@EntityScan(basePackages = "com.github.sibdevtools")
@EnableScheduling
@SpringBootApplication(scanBasePackages = {
        "com.github.sibdevtools.web.app.mocks.config",
        "com.github.sibdevtools"
})
public class StandaloneApplication {
    /**
     * Application entry point
     *
     * @param args arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(StandaloneApplication.class, args);
    }

}
