import exceptions.ExportCSVTODBException;
import helper.CSVLoader;
import utility.Constants;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


/**
 * Created By: garvit
 * Date: 2/5/19
 **/

public class ApplicationStart {

    public static void main(String[] args) {

        try {
            // TODO : File name with relative path
            ApplicationStart applicationStart = new ApplicationStart();
            InputStream inputStream = applicationStart.getInputStream();
            Properties properties = new Properties();
            properties.load(inputStream);

            final Driver driver = (Driver) Class.forName(properties.getProperty(Constants.driverName)).newInstance();
            Connection connection = DriverManager.getConnection(properties.getProperty(Constants.connectionUrl));
            CSVLoader loader = new CSVLoader(connection);
            loader.loadCSV(properties.getProperty(Constants.fileName) , properties.getProperty(Constants.tableName),false);

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ExportCSVTODBException e) {
            e.printStackTrace();
        }
    }

    private InputStream getInputStream() {
        return this.getClass().getClassLoader()
                .getResourceAsStream("database.properties");
    }
}
