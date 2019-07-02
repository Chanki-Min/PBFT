package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.SQLException;

public class CountMessages extends Operation {
    protected CountMessages(PublicKey clientInfo) {
        super(clientInfo);
    }

    @Override
    public Object execute(Object obj) {
        Logger logger = (Logger) obj;


        String[] table_name = {"Preprepares", "Prepares", "Commits", "Executed"};
        String base_query = "SELECT COUNT(*) FROM ";

        int[] result = new int[table_name.length];

        for (int i = 0; i < result.length; i++) {


            try (var pstmt = logger.getPreparedStatement(base_query + table_name[i])) {
                var ret = pstmt.executeQuery();
                if (ret.next()) {
                    result[i] = ret.getInt(1);
                } else {
                    result[i] = -987654321;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }


        }
        return result;
    }
}
