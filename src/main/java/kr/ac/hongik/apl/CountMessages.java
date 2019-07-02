package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.PreparedStatement;
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
        int result = -1;
        try {
            PreparedStatement pstmt = logger.getPreparedStatement(base_query + table_name[0]);

            var ret = pstmt.executeQuery();
            if (ret.next()) {
                result = ret.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }
}
