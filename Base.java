package market.util.database;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * 
 * @author jorgesaw
 */

// Clase para trabajar con el patròn Singleton.
// Nuestra capa de comunicacdión.
public class Base {
	// Almacena internamente el proveedor de DB.
	// private static JdbcDB _proveedor;
	// Usado para los callbacks
	// private Vector params;
	// Almacena la instancia para el Singleton
	private static Base proveedor;
	// Almacena el objeto para la consulta
	private PreparedStatement stmt;
	// Guarda internamente el objeto de conexión.
	private Connection conn;

	private boolean _conectado = false;

	private int numError;

	// Constructor privado
	private Base(String idProveedor) {
		String host = "localhost:3306";
		String dbName = "market";
		String user = "root";
		String pass = "";
		String URL;
		String Driver;

		switch (idProveedor) {
		case "Mysql":
			Driver = "com.mysql.jdbc.Driver";
			URL = "jdbc:mysql://" + host + "/" + dbName;
			// _proveedor = new Base("Mysql");
			// _proveedor.conn = _proveedor.dbConnect(Driver,
			// URL, user, pass);
			dbConnect(Driver, URL, user, pass);
			break;
		case "Derby":
			Driver = "com.mysql.jdbc.Driver";
			URL = "jdbc:" + host + ":" + dbName;
			// _proveedor = new Base("Derby");
			// _proveedor.conn = _proveedor.dbConnect(Driver, URL, user,
			// pass);
			dbConnect(Driver, URL, user, pass);
			break;
		default:
			System.err.println("La clase buscada con ese "
					+ "parámetro no existe.");
			break;
		}
		if (!isConnected()) {
			System.err.println("No se pudo conectar al " + "servidor de DB");
		}

	}

	// MÃ©todo del Singleton que crea o devuelve la instancia
	public static Base getConnection(String idProveedor) {
			// *********
			// Mucho cuidado, si paramos el servidor y luego lo hacemos andar de
			// nuevo
			// nunca hay conexión de nuevo porque _proveedor no es null
			// pero _conn si es null!!! (ya que cerramos la conexión
			// que es el objeto que devolvemos). Por eso en el if le agrego
			// que retorne conn cuando éste sea != null
			// *************
		if(Base.proveedor == null) {
			proveedor = new Base(idProveedor);
//			System.out.println("No existe.");
		}
		return Base.proveedor;
	}

	@SuppressWarnings("finally")
	public Connection dbConnect(String Driver, String URL, String user,
			String pass) {
		// String Driver = "com.mysql.jdbc.Driver";
		// String URL = "jdbc:mysql://" + host + "/" + dbName;
		conn = null;
		try {
			Class.forName(Driver);
			conn = DriverManager.getConnection(URL, user, pass);
			System.out.println("Conexión exitosa...");
			if (conn != null) {
				_conectado = true;
			}
		} catch (ClassNotFoundException e) {
			Logger.getLogger(JdbcDB.class.getName()).log(Level.SEVERE, null, e);
			System.err.println("ERROR " + e.getMessage());
		} catch (SQLException ex) {
			System.err.println("N° ERROR: " + ex.getErrorCode() + "\n");
			System.err.println("ERROR " + ex.getMessage());
			numError = (int) ex.getErrorCode();
		} finally {
			return conn;
		}

	}

	public int getNumError() {
		return numError;
	}

	public List<Object[]> executeQuery(String q, Map<Integer, Object> params) {
		ResultSet result = this.sendQuery(q, params);
		ArrayList<Object[]> l = null;
		if (result != null) {
			l = ((ArrayList<Object[]>) dbResultToArrayList(result));
		}
		// System.out.println("Devuelve null.");
//		return null;
		return l;
	}

	public int executeUpdate(String q, Map<Integer, Object> params) {
		return this.sendUpdate(q, params);
	}

	@SuppressWarnings("finally")
	private ResultSet sendQuery(String q, Map<Integer, Object> params) {
		ResultSet rs = null;

		try {
			// Preparamos la conexiòn
			stmt = conn.prepareStatement(q);
			// Saneamos los datos
			prepararData(params);
			// Ejecutamos la consulta
			rs = stmt.executeQuery();
			System.out.println("Consulta exitosa...");
		} catch (SQLException ex) {
			System.err.println("N° ERROR " + ex.getErrorCode());
			System.err.println("ERROR " + ex.getMessage());
			numError = (int) ex.getErrorCode();
		} finally {
			if (stmt != null && rs != null) {
				return rs;
			}
			return null;
		}
	}

	private Integer sendUpdate(String q, Map<Integer, Object> params) {
		int result = -1;
		try {
			// Preparamos la conexión
			stmt = conn.prepareStatement(q);
			// Saneamos los datos
			if (params.size() > 0) {
				prepararData(params);
			}
			// Ejecutamos la consulta
			result = stmt.executeUpdate();
			System.out.println("Consulta exitosa...");
		} catch (SQLException ex) {
			Logger.getLogger(JdbcDB.class.getName())
					.log(Level.SEVERE, null, ex);
			System.err.println("Error al intentar ejecutar la consulta...");
			System.out.println("Nº ERROR: " + ex.getErrorCode());
			System.err.println("ERROR " + ex.getMessage());
			numError = (int) ex.getErrorCode();
		} finally {
			return result;
		}
	}

	@SuppressWarnings({ "finally" })
	public ArrayList<Object[]> dbResultToArrayList(ResultSet result) {
		ArrayList<Object[]> listDB = new ArrayList<Object[]>();

		try {
			int numColumnas = result.getMetaData().getColumnCount();
			String tipoCol;
			while (result.next()) {
				Object[] arrayRowDB = new String[numColumnas];
				for (int i = 0; i < numColumnas; i++) {
					tipoCol = result.getMetaData().getColumnTypeName(i + 1);
					System.out.println("La columna: " + i + " es del tipo: "
							+ tipoCol);
					arrayRowDB[i] = result.getString(i + 1).toString();
				}
				listDB.add(arrayRowDB);
			}

		} catch (SQLException ex) {
			Logger.getLogger(JdbcDB.class.getName())
					.log(Level.SEVERE, null, ex);
			System.out.println("Nº ERROR: " + ex.getErrorCode());
			numError = (int) ex.getErrorCode();
		} finally {
			if (listDB != null) {
				return listDB;
			}
			return null;
		}
	}

	public boolean isConnected() {
		return _conectado;
	}

	private void prepararData(Map<Integer, Object> params) throws SQLException {
		Iterator<Entry<Integer, Object>> iter = params.entrySet().iterator();
		int i = 0;
		String elemento;
		Class<? extends Object> classElemento;
		while (iter.hasNext()) {
			// for (int i = 0; i < params.size();i++) {
			i++;
			// Class classElemento = params.get(i).getClass();
			// String elemento = classElemento.getSimpleName();
			@SuppressWarnings("rawtypes")
			Map.Entry e = (Map.Entry) iter.next();
			// System.out.println(e.getKey() + " - " + e.getValue());
			classElemento = e.getValue().getClass();

			elemento = classElemento.getSimpleName();
			System.out.println(elemento);
			switch (elemento) {
			case "Boolean":
				stmt.setBoolean(i, (Boolean) e.getValue());
				// _stmt.setBoolean(i + 1, (Boolean) params.get(i));
				break;
			case "Double":
				stmt.setDouble(i, (Double) e.getValue());
				break;
			case "Float":
				stmt.setFloat(i, (Float) e.getValue());
				break;
			case "Integer":
				stmt.setInt(i, (Integer) e.getValue());
				break;
			case "Long":
				stmt.setLong(i, (Long) e.getValue());
				break;
			case "String":
				stmt.setString(i, (String) e.getValue());
				break;
			case "Date":
				stmt.setDate(i, (Date) e.getValue());
				break;
			case "Time":
				stmt.setTime(i, (Time) e.getValue());
				break;
			case "null":
				stmt.setNull(i, (Integer) null);
				break;
			default:
				// _stmt.setNull(i, (Integer) null);
				System.out.println("Tipo no especificado...");
				break;
			}
		}
		// }

	}

	public static void main(String[] args) {

		/*
		 * System.out.println(DBCapa._conn); DBCapa db =
		 * DBCapa.getConnection("Mysql"); System.out.println(DBCapa._conn);
		 * 
		 * Map<Integer, Object> listaData = new HashMap(); listaData.put(0,
		 * 2719); listaData.put(1, "Yerba Aguantadora"); listaData.put(2,
		 * 11.23); int retorno = db.executeUpdate("INSERT INTO articulo VALUES "
		 * + "(?,?,?)", listaData);
		 * System.out.println("Retorno de INSERT: "+retorno);
		 */

		// DBCapa db = DBCapa.getConnection("Mysql");
		// List lista = new ArrayList();
		// lista = db.executeQuery("SELECT * FROM venta", new HashMap());

		Base db = Base.getConnection("Mysql");
		// Base db2 = Base.getConnection("Mysql");
		// List lista = new ArrayList();
		// lista = db.executeQuery("SELECT COUNT(*) FROM venta", new HashMap());
		// String[] row = (String[])lista.get(0);
		// int n = db.executeUpdate("SELECT COUNT(*) from venta", new
		// HashMap());
		// System.out.println("El primer campo de la fila tiene el valor: "
		// + row[0]);

		// List lista = new ArrayList();
		int lista = db.executeUpdate("INSERT INTO articulo " +
				"VALUES (5656, 'Repetido', 7.89)", new HashMap<Integer, Object>());
		String mensaje = "";
		if (lista == -1) {
			mensaje = "NULL";
		} else if (lista == 0) {
			mensaje = "No existe en la DB";
		} else {
			mensaje = "EXISTE " + lista;
			// String[] row = (String[])lista.get(0);
		}

		System.out.println("El mensaje es: " + mensaje);
		System.out.println("El N° de error es: " + db.getNumError());
		System.out.println(db.isConnected());
	}

}
