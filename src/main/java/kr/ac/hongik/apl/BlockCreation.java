package kr.ac.hongik.apl;

import java.security.PublicKey;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.function.Function;

public class BlockCreation extends Operation {
	private final BlockPayload blockPayload;
	private Function<String, PreparedStatement> sqlAccessor = null;

	protected BlockCreation(PublicKey clientInfo, String artHash, String seller, String buyer, long price, long duration) {
		this(clientInfo, new BlockPayload(artHash, seller, buyer, price, duration));
	}

	protected BlockCreation(PublicKey clientInfo, BlockPayload blockPayload) {
		super(clientInfo, false);
		this.blockPayload = blockPayload;
	}

	/**
	 * @param sqlAccessor If the replica identify that this message is BlockCreation instance,
	 *                    then the replica insert the sql preparedStatement function.
	 */
	public void setSqlAccessor(Function<String, PreparedStatement> sqlAccessor) {
		this.sqlAccessor = sqlAccessor;
	}

	/**
	 * @return ({ certNumber, certPiece }, { certNumber, certPiece }, merkle tree root)
     * @param obj
	 */
	@Override
    public Object execute(Object obj) {

		createTableIfNotExists();

		String prevHash = getLatestheader();

		String header = Util.hash(prevHash + blockPayload);

		insertBlock(header, blockPayload, this.getTimestamp());

		return header;

	}

	private void createTableIfNotExists() {
		if (checkIfExists())
			return;

		String query = "CREATE TABLE Blocks" +
				"(header TEXT, " +
				"work TEXT NOT NULL, " +
				"seller TEXT NOT NULL, " +
				"buyer TEXT NOT NULL, " +
				"price INT NOT NULL, " +
				"txnTime INT NOT NULL, " +
				"duration INT, " +

				"PRIMARY KEY(header))";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.execute();

			/******         CREATE FIRST BLOCK          ********/

			query = "INSERT INTO Blocks VALUES(?, ?, ?, ?, ?, ?, ?)";
			try (var pstmt1 = getPreparedStatement(query)) {
				pstmt1.setString(1, "first");
				pstmt1.setString(2, "first");
				pstmt1.setString(3, "first");
				pstmt1.setString(4, "first");
				pstmt1.setLong(5, -1);
				pstmt1.setLong(6, 0);
				pstmt1.setLong(7, 0);

				pstmt1.execute();
			}
		} catch (SQLException | RuntimeException e) {
			if (e instanceof SQLException)
				System.err.println("ERROR CODE: " + ((SQLException) e).getErrorCode());
			e.printStackTrace();
		}
	}

	private boolean checkIfExists() {
		String query = "SELECT 1 FROM sqlite_master WHERE type='table' AND name='Blocks'";
		try (var pstmt = getPreparedStatement(query)) {
			try (var ret = pstmt.executeQuery()) {
				if (ret.next())
					return ret.getBoolean(1);
				else
					return false;
			}
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	private String getLatestheader() {
		String query = "SELECT B.header " +
				"FROM Blocks B " +
				"WHERE B.txnTime = (SELECT MAX(B1.txnTime) " +
				"                   FROM Blocks B1 )";
		try (var pstmt = getPreparedStatement(query)) {
			try (var ret = pstmt.executeQuery()) {
				ret.next();
				return ret.getString(1);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private void insertBlock(String header, BlockPayload payload, long txnTime) {
		String query = "INSERT INTO Blocks " +
				"VALUES(?, ?, ?, ?, ?, ?, ?)";
		try (var pstmt = getPreparedStatement(query)) {
			pstmt.setString(1, header);
			pstmt.setString(2, payload.getArtHash());
			pstmt.setString(3, payload.getSeller());
			pstmt.setString(4, payload.getBuyer());
			pstmt.setLong(5, payload.getPrice());
			pstmt.setLong(6, txnTime);
			pstmt.setLong(7, payload.getDuration());

			pstmt.execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	private PreparedStatement getPreparedStatement(String query) {
		return sqlAccessor.apply(query);
	}


}
