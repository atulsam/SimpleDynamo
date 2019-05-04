package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	FeedReaderDbHelper dbHelper;
	private String TABLE_NAME = "dynamo";
	private String COLUMN_NAME_KEY = "key";
	private String COLUMN_NAME_VALUE = "value";
	private String COLUMN_NAME_SOURCE = "source";
	private String COLUMN_NAME_VERSION = "version";
	static final int SERVER_PORT = 10000;
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
//	static String[] REMOTE_PORT = new String[]{"11108", "11112", "11116", "11120", "11124"};

	private String myPort =  "";
	static String myId="";
	static TreeMap<String,Node> ring = new TreeMap<String, Node>();
	private HashMap<String,String> portIdMap = new HashMap<String, String>();
	private HashMap<String,String> portMap = new HashMap<String, String>();

	private StringBuilder receivedKeyValueQuery;
	private StringBuilder sendKeyValueQuery;
	boolean insertKey = false;
	boolean insertedKey = false;
	private StringBuilder myKeyVal;
	boolean signalQuery1=false;
	boolean signalQuery2=false;

	@Override
	public boolean onCreate() {

		dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		db.delete(TABLE_NAME, null, null);

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf((Integer.parseInt(portStr) * 2));
		switch(Integer.parseInt(myPort)){
			case(11108): myId = "5554"; break;
			case(11112): myId = "5556"; break;
			case(11116): myId = "5558"; break;
			case(11120): myId = "5560"; break;
			case(11124): myId = "5562"; break;
		}
		portIdMap.put("5554","11108");
		portIdMap.put("5556","11112");
		portIdMap.put("5558","11116");
		portIdMap.put("5560","11120");
		portIdMap.put("5562","11124");

		portMap.put("11108","5554");
		portMap.put("11112","5556");
		portMap.put("11116","5558");
		portMap.put("11120","5560");
		portMap.put("11124","5562");

		createDhtRing();

		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}

		return false;
	}



	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		return 0;
	}

	@Override
	public String getType(Uri uri) {
		return null;
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		try{
			String actualKey = (String) values.get(COLUMN_NAME_KEY);
			String value = (String) values.get(COLUMN_NAME_VALUE);
			String hash = genHash(actualKey);

			Map.Entry e1 = ring.lowerEntry(hash);
			Map.Entry e2 = ring.higherEntry(hash);
			Node n;

			if(e2 != null && e1 != null){
				n = (Node) e2.getValue();
			}else{
				Map.Entry eLow = ring.firstEntry();
				n = (Node) eLow.getValue();
			}
			Node pred = n.getPred();
			Node predpred = n.getPred().getPred();

			if(n.port.equalsIgnoreCase(myPort)){
				String source = myPort;
				handleInsert(uri,values,source);
				Log.v("INSERT_SELF: ", n.key+"~"+pred.key+"~"+predpred.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("INSERT_KEY",actualKey,value,n.port,pred.port,predpred.port);
				while(!insertKey){}
			}else{
				Log.v("INSERT_SEND: ", n.key+"~"+pred.key+"~"+predpred.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("SEND_KEY",actualKey,value,n.port, pred.port,predpred.port);
			}
		}catch (NoSuchAlgorithmException ne){
			ne.printStackTrace();
		}

		return null;
	}

    public long handleInsert(Uri uri, ContentValues values, String source){
		int version = 1;
        long newRowId;
		String sourceHash ="";
		try {
			sourceHash = genHash(portMap.get(source));
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		String key = (String) values.get(COLUMN_NAME_KEY);
        values.put(COLUMN_NAME_SOURCE,sourceHash);
		values.put(COLUMN_NAME_VERSION,version);

        SQLiteDatabase db = dbHelper.getWritableDatabase();
        newRowId = db.replaceOrThrow(TABLE_NAME, null, values);
        Log.v("Inserted Key: ", key);
        return newRowId;
    }


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

		Cursor cursor = null;
//		myKeyVal = new StringBuilder();
		signalQuery1 = false;
		// https://stackoverflow.com/questions/10600670/sqlitedatabase-query-method
//		String requestedPort= "";
//		String whereClause = COLUMN_NAME_KEY + " = ? ";
//		String[] whereArgs = new String[]{selection};

		try{
			if(selection.equalsIgnoreCase("*")){

			}else if(selection.equalsIgnoreCase("@")){

			}else{
				String keyHashed = genHash(selection);
				Map.Entry e1 = ring.lowerEntry(keyHashed);
				Map.Entry e2 = ring.higherEntry(keyHashed);
				Node n;

				if(e2 != null && e1 != null){
					n = (Node) e2.getValue();
				}else{
					Map.Entry eLow = ring.firstEntry();
					n = (Node) eLow.getValue();
				}

				Node pred = n.getPred();
				Node predpred = n.getPred().getPred();

				if(n.port.equalsIgnoreCase(myPort)){
					String source = myPort;
					signalQuery1 = false;
					signalQuery2 = false;
					Cursor cur = handleQuery(uri,selection,source);
					Log.v("QUERY_SELF: ", n.key+"~"+pred.key+"~"+predpred.key);
					ClientTask clientTask = new ClientTask();
					clientTask.doInBackground("QUERY_KEY",selection,n.port,pred.port,predpred.port);
					while(!signalQuery1){}
					MatrixCursor mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
					Cursor newcursor = mergeCursor(cur, mc);
					signalQuery2 = true;
					return findLatestKeys(newcursor);
				}else{
					Log.v("QUERY_SEND: ", n.key+"~"+pred.key+"~"+predpred.key);
					ClientTask clientTask = new ClientTask();
					clientTask.doInBackground("QUERY_SEND_KEY",selection,n.port, pred.port,predpred.port);
					while(!signalQuery1){}
					MatrixCursor mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
					Log.v("Sent Key : ", selection+" port:"+n.port);
					signalQuery2 = true;
					return findLatestKeys(mc);
				}
			}
		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

		return cursor;
	}

	private Cursor findLatestKeys(Cursor cursor) {
		HashMap<String,KeyStore> keyVal = new HashMap<String, KeyStore>();
		String[] colNames = {"key","value"};
		MatrixCursor cur = new MatrixCursor(colNames);

		while(cursor.moveToNext()){
			if(!keyVal.containsKey(cursor.getString(0))){
				keyVal.put(cursor.getString(0),new KeyStore(cursor.getString(0),
							cursor.getString(1),cursor.getString(2),cursor.getString(3)));
			}else{
				KeyStore k = keyVal.get(cursor.getString(0));
				if(Integer.parseInt(k.version) < Integer.parseInt(cursor.getString(3))){
					keyVal.put(cursor.getString(0),new KeyStore(cursor.getString(0),
							cursor.getString(1),cursor.getString(2),cursor.getString(3)));
				}
			}
		}
		for(Map.Entry keys : keyVal.entrySet()){
			KeyStore k = (KeyStore) keys.getValue();
			String[] row = {k.key,k.value,k.source,k.version};
			cur.addRow(row);
		}
		return cur;
	}
	class KeyStore{
		String key;
		String value;
		String source;
		String version;

		public KeyStore(String key, String value, String source, String version) {
			this.key = key;
			this.value = value;
			this.source = source;
			this.version = version;
		}
	}

	private MatrixCursor mapToCurReceivedKeyQuery(StringBuilder receivedKeyValueQuery) {
		String[] colNames = {"key","value","source","version"};
		MatrixCursor cursor = new MatrixCursor(colNames);
		Log.v("getKeyValuePairCursor1", receivedKeyValueQuery+"~"+myId);
		if(receivedKeyValueQuery.length() >0){
			String[] keyValList = receivedKeyValueQuery.toString().split("#");
			for(String str : keyValList){
				String[] keyVal = str.split("@");
				cursor.addRow(keyVal);
			}
		}
		Log.v("getKeyValuePairCursor2", cursor.getCount()+"");
		return cursor;
	}

	private MatrixCursor mergeCursor(Cursor cur , MatrixCursor c1){

		Log.v("mergeCursor", cur.getCount()+"~"+c1.getCount());

		while(cur.moveToNext()){
			String[] keyVal = {cur.getString(0),cur.getString(1),cur.getString(2),cur.getString(3)};
			c1.addRow(keyVal);
		}

		return c1;
	}

	public Cursor handleQuery(Uri uri, String selection, String source){
		Cursor cursor;
		//myKeyVal = new StringBuilder();
		sendKeyValueQuery = new StringBuilder();
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		String whereClause = COLUMN_NAME_KEY + " = ? and "+COLUMN_NAME_SOURCE+" = ? ";
		if(!selection.equals("*") && !selection.equals("@")){
			String[] whereArgs = new String[]{selection,source};
			cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);
		}else {
			String[] whereArgs = new String[]{"*",source};
			cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);
		}
		while(cursor.moveToNext()){
			String row = cursor.getString(0)+"@"+cursor.getString(1)+"@"+cursor.getString(2)
					+"@"+cursor.getString(3)+"#";
			sendKeyValueQuery.append(row);
		}
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		String str = "key = '"+selection+"'";
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		int count = db.update(TABLE_NAME, values, str, selectionArgs);
//        Log.v("update Row: ", count+"");
		return count;
	}


	/*http://developer.android.com/reference/android/os/AsyncTask.html*/
	class ServerTask extends AsyncTask<ServerSocket, String, Void> {
		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket serverSocket = sockets[0];
			while (true) {
				try {
					Socket sc = serverSocket.accept();
					/*https://stackoverflow.com/questions/28187038/tcp-client-server-program-datainputstream-dataoutputstream-issue*/
					DataInputStream input = new DataInputStream(sc.getInputStream());
					String recStr = input.readUTF();
					String[] rect = recStr.split("~");
					String reqType = rect[0];
					//                    Log.v("****Server ReQ****",recStr);
					if(reqType.equalsIgnoreCase("INSERT_KEY") || reqType.equalsIgnoreCase("SEND_KEY")){
						String key = rect[1];
						String val = rect[2];
						String source = rect[3];
						publishProgress("INSERT_KEY",key,val,source);
						while(!insertedKey){}
						String replyToClient = "INSERT_KEY";
						DataOutputStream output = new DataOutputStream(sc.getOutputStream());
						output.writeUTF(replyToClient);
					}

				} catch (SocketTimeoutException e) {
					Log.e(TAG, "ServerTask SocketTimeoutException");
				} catch (IOException e) {
					Log.e(TAG, "ServerTask IOException");
				} catch (Exception e) {
					Log.e(TAG, "ServerTask Other Exception ");
					e.printStackTrace();
				}
			}
		}

		@Override
		protected void onProgressUpdate(String... strings) {
			Log.v("PublishProgress",strings[0]+"~"+strings[1]);
			OtherTask other = new OtherTask();
			other.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR,strings);
		}
	}


	class OtherTask extends AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... strings) {
			String param = strings[0];
			if(param.equalsIgnoreCase("INSERT_KEY")){
				insertedKey = false;
				String key = strings[1];
				String value = strings[2];
				String source = strings[3];
				ContentValues cv = new ContentValues();
				cv.put(COLUMN_NAME_KEY,key);
				cv.put(COLUMN_NAME_VALUE,value);
				Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				handleInsert(mUri, cv,source);
				insertedKey = true;
			}

			return null;
		}
	}


	class ClientTask extends AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... msgs) {
			DataInputStream ackRec;
			DataOutputStream output;
			String msg = msgs[0];
			try{
				if(msg.equalsIgnoreCase("QUERY_KEY") || msg.equalsIgnoreCase("QUERY_SEND_KEY")){
					signalQuery1 = false;
					receivedKeyValueQuery = new StringBuilder();
					String key = msgs[1];
					String val = msgs[2];
					String source = msgs[3];
					ArrayList<String> port = new ArrayList<String>();
					if(msg.equalsIgnoreCase("QUERY_KEY")){
						port.add(msgs[4]);
						port.add(msgs[5]);
					}else{
						port.add(msgs[3]);
						port.add(msgs[4]);
						port.add(msgs[5]);
					}

					for(String p : port){
						try{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(p));
							socket.setSoTimeout(5000);
							String msgToSend = msg+"~"+key+"~"+source;
                    		Log.v("Client Request:",msgToSend+"~"+p);
							output = new DataOutputStream(socket.getOutputStream());
							output.writeUTF(msgToSend);

							ackRec = new DataInputStream(socket.getInputStream());
							String ackStr = ackRec.readUTF();
                    		Log.v("Client Ack from Server:",ackStr+"~"+p);
							String[] receivedInfo = ackStr.split("~");

							if(receivedInfo[0].equals("QUERY_KEY")){
								signalQuery1 = true;
								receivedKeyValueQuery.append(receivedInfo[1]);
								output.close();
								ackRec.close();
								socket.close();
							}
						}catch(SocketTimeoutException e){
							Log.e(TAG, "ClientTask socket SocketTimeoutException");
						}catch(IOException e){
							Log.e(TAG, "ClientTask socket IOException");
						}catch(Exception e){
							e.printStackTrace();
							Log.e(TAG, "ClientTask Exception Initial");
						}
					}
				}else if(msg.equalsIgnoreCase("INSERT_KEY") || msg.equalsIgnoreCase("SEND_KEY")){
					insertKey = false;
					String key = msgs[1];
					String val = msgs[2];
					String source = msgs[3];
					ArrayList<String> port = new ArrayList<String>();
					if(msg.equalsIgnoreCase("INSERT_KEY")){
						port.add(msgs[4]);
						port.add(msgs[5]);
					}else{
						port.add(msgs[3]);
						port.add(msgs[4]);
						port.add(msgs[5]);
					}

					for(String p : port){
						try{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(p));
							socket.setSoTimeout(5000);
							String msgToSend = msg+"~"+key+"~"+val+"~"+source;
							Log.v("Client Request:",msgToSend+"~"+p);
							output = new DataOutputStream(socket.getOutputStream());
							output.writeUTF(msgToSend);

							ackRec = new DataInputStream(socket.getInputStream());
							String ackStr = ackRec.readUTF();
							Log.v("Client Ack from Server:",ackStr+"~"+p);
							String[] receivedInfo = ackStr.split("~");

							if(receivedInfo[0].equals("INSERT_KEY")){
								insertKey = true;
								output.close();
								ackRec.close();
								socket.close();
							}
						}catch(SocketTimeoutException e){
							Log.e(TAG, "ClientTask socket SocketTimeoutException");
						}catch(IOException e){
							Log.e(TAG, "ClientTask socket IOException");
						}catch(Exception e){
							e.printStackTrace();
							Log.e(TAG, "ClientTask Exception Initial");
						}
					}
				}
			}catch(Exception e){
				e.printStackTrace();
				Log.e(TAG, "ClientTask Exception");
			} finally{

			}
			return null;
		}
	}


	private Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	public void createDhtRing(){
		try{
			Node n1 = new Node("5562",genHash("5562"),portIdMap.get("5562"));
			Node n2 = new Node("5556",genHash("5556"),portIdMap.get("5556"));
			Node n3 = new Node("5554",genHash("5554"),portIdMap.get("5554"));
			Node n4 = new Node("5558",genHash("5558"),portIdMap.get("5558"));
			Node n5 = new Node("5560",genHash("5560"),portIdMap.get("5560"));

			n1.setPred(n5);
			n1.setSucc(n2);
			n2.setPred(n1);
			n2.setSucc(n3);
			n3.setPred(n2);
			n3.setSucc(n4);
			n4.setPred(n3);
			n4.setSucc(n5);
			n5.setPred(n4);
			n5.setSucc(n1);

			ring.put(genHash(n1.key),n1);
			ring.put(genHash(n2.key),n2);
			ring.put(genHash(n3.key),n3);
			ring.put(genHash(n4.key),n4);
			ring.put(genHash(n5.key),n5);

		}catch (NoSuchAlgorithmException ne){
			ne.printStackTrace();
		}

	}
	/*https://developer.android.com/training/data-storage/sqlit*/
	public class FeedReaderDbHelper extends SQLiteOpenHelper {
		// If you change the database schema, you must increment the database version.
		public static final int DATABASE_VERSION = 1;
		public static final String DATABASE_NAME = "SimpleDynamo";
		private final String SQL_CREATE_ENTRIES ="CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ( "
				+ COLUMN_NAME_KEY+" TEXT,"
				+ COLUMN_NAME_VALUE+" TEXT,"
				+ COLUMN_NAME_SOURCE+" TEXT,"
				+ COLUMN_NAME_VERSION + " TEXT)";

		public FeedReaderDbHelper(Context context) {
			super(context, DATABASE_NAME, null, DATABASE_VERSION);

		}
		@Override
		public void onCreate(SQLiteDatabase db) {
			db.execSQL(SQL_CREATE_ENTRIES);
		}
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onCreate(db);
		}
		public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onUpgrade(db, oldVersion, newVersion);
		}
	}

	class Node{
		String key;
		String hash;
		String port;
		Node pred;
		Node succ;

		Node(String key, String hash, String port){
			this.key = key;
			this.hash = hash;
			this.port = port;
			this.pred = null;
			this.succ = null;
		}

		Node(String key, String hash, String port, Node pred, Node succ){
			this.key = key;
			this.hash = hash;
			this.port = port;
			this.pred = pred;
			this.succ = succ;
		}

		public String getPort() { return port; }

		public void setPort(String port) {
			this.port = port;
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public String getHash() {
			return hash;
		}

		public void setHash(String hash) {
			this.hash = hash;
		}

		public Node getPred() {
			return pred;
		}

		public void setPred(Node pred) {
			this.pred = pred;
		}

		public Node getSucc() {
			return succ;
		}

		public void setSucc(Node succ) {
			this.succ = succ;
		}
	}
}
