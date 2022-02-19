package hbase.test.utils;

import hbase.connector.HBaseConnector;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static java.util.Arrays.asList;

public class HBaseTestJunitExtension implements
        BeforeAllCallback, AfterAllCallback, ParameterResolver, ExecutionCondition {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestJunitExtension.class);
    private HBaseTestInstance testInstance;
    private Properties properties;

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext extensionContext) {
        String name = HBaseTestInstanceSingleton.getName().orElse("");

        if (name.isEmpty()) {
            return ConditionEvaluationResult.disabled("No test instance configured");
        } else {
            return ConditionEvaluationResult.enabled("Test instance is set to '" + name + "'");
        }
    }

    @Override
    public void beforeAll(ExtensionContext extensionContext) throws IOException {
        testInstance = HBaseTestInstanceSingleton.instance();
        properties = testInstance.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                testInstance.close();
            } catch (Exception e) {
                LOGGER.error("Failed to close test instance", e);
            }
        }));
    }

    @Override
    public void afterAll(ExtensionContext extensionContext) throws IOException {
        testInstance.close();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> type = parameterContext.getParameter().getType();
        List<Class<?>> supportedTypes = asList(
                TableName.class,
                Properties.class,
                HBaseTestInstance.class,
                HBaseConnector.class
        );
        return supportedTypes.stream().anyMatch(type::isAssignableFrom);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (type.isAssignableFrom(TableName.class)) {
            return TableName.valueOf(testInstance.tempTableName());
        } else if (type.isAssignableFrom(Properties.class)) {
            return properties;
        } else if (type.isAssignableFrom(HBaseConnector.class)) {
            return testInstance.connector();
        } else if (type.isAssignableFrom(HBaseTestInstance.class)) {
            return testInstance;
        } else {
            throw new ParameterResolutionException("No converter for " + type);
        }
    }

}
