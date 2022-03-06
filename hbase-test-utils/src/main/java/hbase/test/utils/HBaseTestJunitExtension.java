package hbase.test.utils;

import edu.umd.cs.findbugs.annotations.Nullable;
import hbase.test.utils.interfaces.HBaseTestInstance;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
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

import static hbase.test.utils.HBaseTestHelpers.newConnection;
import static java.util.Arrays.asList;

public class HBaseTestJunitExtension implements
        BeforeAllCallback, AfterAllCallback, ParameterResolver, ExecutionCondition {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseTestJunitExtension.class);
    private HBaseTestInstance testInstance;
    private Properties properties;
    private Connection connection;

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
    }

    @Override
    public void afterAll(@Nullable ExtensionContext extensionContext) throws IOException {
        LOGGER.info("terminating test instance");
        testInstance.close();
        LOGGER.info("test instance terminated");
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        Class<?> type = parameterContext.getParameter().getType();
        List<Class<?>> supportedTypes = asList(
                TableName.class,
                Properties.class,
                HBaseTestInstance.class,
                Connection.class
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
        } else if (type.isAssignableFrom(HBaseTestInstance.class)) {
            return testInstance;
        } else if (type.isAssignableFrom(Connection.class)) {
            if (connection == null) {
                connection = newConnection(properties);
            }
            return connection;
        } else {
            throw new ParameterResolutionException("No converter for " + type);
        }
    }

}
