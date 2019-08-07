package kr.ac.hongik.apl.Operations;

import kr.ac.hongik.apl.Logger;

import java.security.PublicKey;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GetBlockVerificationLogOperation extends Operation {

	public GetBlockVerificationLogOperation(PublicKey clientInfo){
		super(clientInfo);
	}

	@Override
	public Object execute(Object obj){
		try {
			Logger logger = (Logger) obj;
			return getErrorLogs(logger);
		} catch (SQLException e) {
			List<String> error = new ArrayList<>();
			error.add(String.valueOf(-1L));
			error.add(e.getMessage());
			List<List<String>> errors = new ArrayList<>();
			errors.add(error);
			return errors;
		}
	}

	private List<List<String>> getErrorLogs(Logger logger) throws SQLException{
		String query = "SELECT timestamp, blockNum, entryNum, errorCode FROM VerificationLogs";
		var psmt = logger.getPreparedStatement(query);
		var rs = psmt.executeQuery();

		List<List<String>> logs = new ArrayList<>();
		while (rs.next()){
			List<String> log = new ArrayList<>();
			log.add(String.valueOf(rs.getLong(1)));
			log.add(String.valueOf(rs.getInt(2)));
			log.add(String.valueOf(rs.getInt(3)));
			log.add(rs.getString(4));
			logs.add(log);
		}
		return logs;
	}
}
