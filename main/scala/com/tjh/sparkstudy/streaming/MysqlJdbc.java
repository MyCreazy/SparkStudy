package com.tjh.sparkstudy.streaming;

import com.alibaba.druid.pool.DruidDataSource;
import com.tjh.sparkstudy.hive.Constant;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MysqlJdbc {
    private Connection connection = null;

    public MysqlJdbc(String url, String username, String password) {
        init(url, username, password);
    }

    public MysqlJdbc() {
        init(Constant.MYSQL_URL, Constant.MYSQL_USER, Constant.MYSQL_PASSWORD);
    }

    public void init(String url, String username, String password) {
        try {
            DruidDataSource dataSource = new DruidDataSource();
            //设置连接参数
            dataSource.setUrl(url);
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUsername(username);
            dataSource.setPassword(password);
            //配置初始化大小、最小、最大
            dataSource.setInitialSize(1);
            dataSource.setMinIdle(1);
            dataSource.setMaxActive(20);
            //配置获取连接等待超时的时间
            dataSource.setMaxWait(20000);
            //配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
            dataSource.setTimeBetweenEvictionRunsMillis(20000);
            //防止过期
            dataSource.setValidationQuery("SELECT 1");
            dataSource.setTestWhileIdle(true);
            dataSource.setTestOnBorrow(true);
            //租期时长
            dataSource.setRemoveAbandoned(true);
            dataSource.setRemoveAbandonedTimeout(3600);

            try {
                connection = dataSource.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void execute(String sql) {
        execute(sql, null);
    }

    public void execute(String sql, Object[] params) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);

            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public int insertBatch(String sql, Object[][] params) {
        int index = 0;
        PreparedStatement statement = null;
        try {
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(sql);

            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    Object[] param = params[i];

                    for (int k = 0; k < param.length; ++k) {
                        statement.setObject(k + 1, param[k]);
                    }
                    statement.addBatch();

                    if (i > 0 && i%500 == 0) {
                        int[] array = statement.executeBatch();
                        connection.commit();

                        index += sum(array);
                    }
                }
            }

            int[] array = statement.executeBatch();
            connection.commit();

            index += sum(array);
        } catch (SQLException e) {
            e.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            return index;
        } finally {
            System.out.println(String.format("[Jdbc class] executed sql %s line", index));

            try {
                statement.close();
                connection.setAutoCommit(true);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return index;
    }

    public List<Map<String, String>> query(String sql) {
        return query(sql, null);
    }

    public List<Map<String, String>> query(String sql, Object[] params) {
        try {
            PreparedStatement statement = connection.prepareStatement(sql);

            if (params != null) {
                for (int i = 0; i < params.length; ++i) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            List<Map<String, String>> list =  new ArrayList<>();
            ResultSet rs = statement.executeQuery();

            String[] columns = getColumns(rs);

            while(rs.next()) {
                Map<String, String> map = new HashMap<>();

                for (String column: columns) {
                    map.put(column ,rs.getString(column));
                }

                list.add(map);
            }
            rs.close();
            statement.close();

            return list;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private String[] getColumns(ResultSet rs) throws SQLException {
        List<String> array = new ArrayList<>();

        ResultSetMetaData data = rs.getMetaData();
        for (int i = 1; i <= data.getColumnCount(); i++) {
            array.add(data.getColumnName(i));
        }

        return array.toArray(new String[data.getColumnCount()]);
    }

    private int sum(int[] array) {
        int sum = 0;
        for (int i: array) {
            sum += i;
        }
        return sum;
    }

}
