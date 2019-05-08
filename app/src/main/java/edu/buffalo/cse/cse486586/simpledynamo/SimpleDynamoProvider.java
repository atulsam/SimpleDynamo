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
	static String[] REMOTE_PORT = new String[]{"11108", "11112", "11116", "11120", "11124"};

	private String myPort =  "";
	static String myId="";
	static TreeMap<String,Node> ring = new TreeMap<String, Node>();
	private HashMap<String,String> portIdMap = new HashMap<String, String>();
	private HashMap<String,String> portMap = new HashMap<String, String>();

	private StringBuilder receivedKeyValueQuery;
	private StringBuilder sendKeyValueQuery;
//	private StringBuilder receivedKeyValueQuery;
//	private StringBuilder sendKeyValueQuery;
	boolean insertKey = false;
	boolean insertedKey = false;
	boolean signalQuery1=false;
	boolean signalQuery2=false;
	boolean signalQuery3=false;
	private boolean signalDelete1 = false;
	private boolean signalDelete2 = false;
	private boolean signalInit1 = false;
	private boolean signalInit2 = false;
	boolean tableExist =true;
	@Override
	public boolean onCreate() {

		dbHelper = new FeedReaderDbHelper(getContext());
		SQLiteDatabase db = dbHelper.getWritableDatabase();
//		boolean isDB = dbHelper.checkTable(db);
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
		Log.v("RING",printRing());
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
			if(tableExist){
				finishStaringAvd();
			}
		} catch (IOException e) {
			Log.e(TAG, "Can't create a ServerSocket");
			e.printStackTrace();
		}
		return false;
	}


	public void finishStaringAvd(){
		try{
			SQLiteDatabase db = dbHelper.getWritableDatabase();
			Node n = ring.get(genHash(myId));
			Node succ = n.getSucc();
			Node succsucc = n.getSucc().getSucc();
			signalInit1 = false;
			MatrixCursor mc;
			OtherClientTask clientTask = new OtherClientTask();
			clientTask.execute("QUERY_ALL_INIT","ALL",n.port,succ.port,succsucc.port);
			while(!signalInit1){}
			Log.v("QUERY_ALL_INIT2",receivedKeyValueQuery+"~");
			if(receivedKeyValueQuery!=null){
				mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
				storeLatestKeysAndSource(mc,myId);
			}
		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}

	}
	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {

		if(selection.equals("*")){
			signalDelete1 = false;
			Log.v("DELETE_ALL: ", selection);
			handleDelete(uri,selection);
			ClientTask clientTask = new ClientTask();
			clientTask.doInBackground("DELETE_ALL",selection);
			while(!signalDelete1){}
		}else if(selection.equals("@")){
			handleDelete(uri,selection);
		}else{
			String keyHashed = "";
			try {
				keyHashed = genHash(selection);
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
			Map.Entry e1 = ring.lowerEntry(keyHashed);
			Map.Entry e2 = ring.higherEntry(keyHashed);
			Node n;

			if(e2 != null && e1 != null){
				n = (Node) e2.getValue();
			}else{
				Map.Entry eLow = ring.firstEntry();
				n = (Node) eLow.getValue();
			}

			Node succ = n.getSucc();
			Node succsucc = n.getSucc().getSucc();
			if(n.port.equalsIgnoreCase(myPort)){
				signalDelete1 = false;
				handleDelete(uri,selection);
				Log.v("DELETE_KEY_SELF: ", n.key+"~"+succ.key+"~"+succsucc.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("DELETE_KEY",selection,n.port,succ.port,succsucc.port);
				while(!signalDelete1){}
			}else{
				signalDelete1 = false;
//				handleDelete(uri,selection);
				Log.v("DELETE_KEY_SELF: ", n.key+"~"+succ.key+"~"+succsucc.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("DELETE_KEY",selection,n.port,succ.port,succsucc.port);
				while(!signalDelete1){}
			}
		}

		return 0;
	}

	private void handleDelete(Uri uri, String selection) {
		SQLiteDatabase  db = dbHelper.getWritableDatabase();
		if(selection.equals("*") || selection.equals("@")){
			db.delete(TABLE_NAME, null, null);
		}else{
			String whereClause = COLUMN_NAME_KEY + " = ? ";
			String[] whereArgs = new String[]{selection};
			int deletedRow = db.delete(TABLE_NAME, whereClause, whereArgs);
			Log.v("DELETE_HANDLE",deletedRow+"~"+selection);
		}
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
			Node succ = n.getSucc();
			Node succsucc = n.getSucc().getSucc();

			if(n.port.equalsIgnoreCase(myPort)){
				String source = myPort;
				handleInsert(uri,values,source);
				Log.v("INSERT_SELF: ", n.key+"~"+succ.key+"~"+succsucc.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("INSERT_KEY",actualKey,value,n.port,succ.port,succsucc.port);
				while(!insertKey){}
			}else{
				Log.v("INSERT_SEND: ", n.key+"~"+succ.key+"~"+succsucc.key);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("SEND_KEY",actualKey,value,n.port, succ.port,succsucc.port);
			}
		}catch (NoSuchAlgorithmException ne){
			ne.printStackTrace();
		}

		return null;
	}

    public long handleInsert(Uri uri, ContentValues values, String source){
		int version = 1;
        long newRowId =0;
		String sourceHash ="";
		try {
			sourceHash = portMap.get(source);
		} catch (Exception e) {
			e.printStackTrace();
		}
		String key = (String) values.get(COLUMN_NAME_KEY);
        values.put(COLUMN_NAME_SOURCE,sourceHash);


        SQLiteDatabase db = dbHelper.getWritableDatabase();

		String whereClause = COLUMN_NAME_KEY + " = ? ";
		String[] whereArgs = new String[]{key};
		Cursor cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);

		if(cursor.getCount() > 0){
			cursor.moveToPosition(0);
			int v = Integer.parseInt(cursor.getString(3));
			values.put(COLUMN_NAME_VERSION,v+1);
			newRowId = db.replaceOrThrow(TABLE_NAME, null, values);
		}else{
			values.put(COLUMN_NAME_VERSION,version);
			newRowId = db.replaceOrThrow(TABLE_NAME, null, values);
		}

        Log.v("Inserted Key: ", key);
        return newRowId;
    }


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {

//		Cursor cursor = null;
//		myKeyVal = new StringBuilder();
		signalQuery1 = false;
		// https://stackoverflow.com/questions/10600670/sqlitedatabase-query-method
//		String requestedPort= "";
//		String whereClause = COLUMN_NAME_KEY + " = ? ";
//		String[] whereArgs = new String[]{selection};

		try{
			if(selection.equalsIgnoreCase("*")){
				signalQuery1 = false;
				signalQuery2 = false;
				Cursor cur = handleQuery(uri,selection,"*");
				Log.v("QUERY_SELF: ", selection);
				ClientTask clientTask = new ClientTask();
				clientTask.doInBackground("QUERY_ALL",selection,"*");
				while(!signalQuery1){}
				MatrixCursor mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
				Cursor newcursor = mergeCursor(cur, mc);
				signalQuery2 = true;
				return findLatestKeys(newcursor);
			}else if(selection.equalsIgnoreCase("@")){
                String source = myPort;
                signalQuery1 = false;
                signalQuery2 = false;
                Node n = ring.get(genHash(myId));
                Node pred = n.getSucc();
                Node predpred = n.getSucc().getSucc();

                Cursor cur = handleQuery(uri,selection,source);
                return convertCursor(cur);
                /*Log.v("QUERY_SELF: ", n.key+"~"+pred.key+"~"+predpred.key);
                ClientTask clientTask = new ClientTask();
                clientTask.doInBackground("QUERY_KEY",selection,n.port,pred.port,predpred.port);
                while(!signalQuery1){}
                MatrixCursor mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
                Cursor newcursor = mergeCursor(cur, mc);
                signalQuery2 = true;
                return findLatestKeys(newcursor);*/
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

				Node pred = n.getSucc();
				Node predpred = n.getSucc().getSucc();

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
					return findThisKey(findLatestKeys(newcursor),selection);
				}else{
					Log.v("QUERY_SEND: ", n.key+"~"+pred.key+"~"+predpred.key);
					ClientTask clientTask = new ClientTask();
					clientTask.doInBackground("QUERY_SEND_KEY",selection,n.port, pred.port,predpred.port);
					while(!signalQuery1){}
					MatrixCursor mc = mapToCurReceivedKeyQuery(receivedKeyValueQuery);
					Log.v("Sent Key : ", selection+" port:"+n.port);
					signalQuery2 = true;
					return findThisKey(findLatestKeys(mc),selection);
				}
			}
		}catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return null;
		}

//		return cursor;
	}

	private Cursor findThisKey(Cursor cursor, String key){
		cursor.moveToPosition(-1);
		String[] colNames = {"key","value"};
		MatrixCursor cur = new MatrixCursor(colNames);
		while(cursor.moveToNext()){
			if(cursor.getString(0).equals(key)){
				String[] row = {cursor.getString(0),cursor.getString(1)};
				cur.addRow(row);
				break;
			}
		}
		Log.v("findThisKey",cur.getCount()+"~");
		return cur;
	}

	private Cursor convertCursor(Cursor cursor){
		Log.v("ConvertCursor",cursor.getCount()+"~");
		String[] colNames = {"key","value"};
		MatrixCursor cur = new MatrixCursor(colNames);
		if(cursor.getCount() >0 ){
			cursor.moveToPosition(0);
			String[] row = {cursor.getString(0),cursor.getString(1)};
			cur.addRow(row);
			while(cursor.moveToNext()){
				Log.v("ConvertCursorROW",cursor.getString(0)+"~"+cursor.getString(1));
				String[] row1 = {cursor.getString(0),cursor.getString(1)};
				cur.addRow(row1);
			}
		}

		Log.v("ConvertCursor",cur.getCount()+"~");
		return cur;
	}
	private void storeLatestKeysAndSource(Cursor cursor,String source) {
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		HashMap<String,KeyStore> keyVal = new HashMap<String, KeyStore>();
		String[] colNames = {"key","value"};
		MatrixCursor cur = new MatrixCursor(colNames);
		Log.v("storeLatestKeys1",cursor.getCount()+"~");

		if(cursor.getCount() >0 ){
			cursor.moveToPosition(-1);
			while(cursor.moveToNext()){
				String ke = cursor.getString(0);
				String v = cursor.getString(1);
				String s = cursor.getString(2);
				String ve = cursor.getString(3);
				if(ke != null && v != null && s != null && ve != null && !ve.equals("null")){
					Log.v("DEBUG",ke+"~"+v+"~"+s+"~"+ve);
					if(!keyVal.containsKey(ke)){
						keyVal.put(ke,new KeyStore(ke,v,s,ve));
					}else{
						KeyStore k = keyVal.get(cursor.getString(0));
						if(k.version!=null && (Integer.parseInt(k.version) < Integer.parseInt(ve))){
							keyVal.put(ke,new KeyStore(ke,v,s,ve));
						}
					}
				}

			}
		}

		Log.v("storeLatestKeys2",keyVal.size()+"~");

		for(Map.Entry keys : keyVal.entrySet()){
			KeyStore k = (KeyStore) keys.getValue();
//			if(k.source.equals(source)){
				String whereClause = COLUMN_NAME_KEY + " = ? ";
				String[] whereArgs = new String[]{k.key};
				Cursor cur1 = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);

				ContentValues values = new ContentValues();
				values.put(COLUMN_NAME_KEY,k.key);
				values.put(COLUMN_NAME_VALUE,k.value);
				values.put(COLUMN_NAME_SOURCE,k.source);

				if(cur1.getCount() <= 0){
					values.put(COLUMN_NAME_VERSION,k.version);
					db.replaceOrThrow(TABLE_NAME, null, values);
				}else{
					int curV = Integer.parseInt(cur1.getString(3));
					int kv = Integer.parseInt(k.version);
					if(kv > curV) values.put(COLUMN_NAME_VERSION,kv+1);
					else	values.put(COLUMN_NAME_VERSION,curV+1);
					db.replaceOrThrow(TABLE_NAME, null, values);
				}

				Log.v("VALUES",values.getAsString(COLUMN_NAME_KEY)+"~"+values.getAsString(COLUMN_NAME_VERSION));
//			}
		}

		Log.v("storeLatestKeys3",keyVal.size()+"~");
	}

	private Cursor findLatestKeys(Cursor cursor) {
		HashMap<String,KeyStore> keyVal = new HashMap<String, KeyStore>();
		String[] colNames = {"key","value"};
		MatrixCursor cur = new MatrixCursor(colNames);
		Log.v("findLatestKeys1",cursor.getCount()+"~");

		if(cursor.getCount() >0 ){
			cursor.moveToPosition(-1);
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
		}
		Log.v("findLatestKeys2",keyVal.size()+"~");
		for(Map.Entry keys : keyVal.entrySet()){
			KeyStore k = (KeyStore) keys.getValue();
			String[] row = {k.key,k.value};
			cur.addRow(row);
		}

		Log.v("findLatestKeys3",cur.getCount()+"~");
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
		Log.v("HANDLE_QUERY",selection+"~"+portMap.get(source));

		sendKeyValueQuery = new StringBuilder();
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		if(source.equals("*")){
			cursor = db.query(TABLE_NAME,null, null, null, null, null, null);
		}else{
			source = portMap.get(source);
			if(selection.equals("*") || selection.equals("@")){
				Log.v("HANDLE_QUERY1",selection+"~"+source);
				cursor = db.query(TABLE_NAME,null, null, null, null, null, null);
			}else if(selection.equals("ALL")){
				String whereClause = COLUMN_NAME_SOURCE+" = ? ";
				String[] whereArgs = new String[]{source};
				Log.v("HANDLE_QUERY2",selection+"~"+source);
				cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);
			}else {
				Log.v("HANDLE_QUERY3",selection+"~"+source);
				String whereClause = COLUMN_NAME_KEY + " = ? and "+COLUMN_NAME_SOURCE+" = ? ";
				String[] whereArgs = new String[]{selection,source};
				cursor = db.query(TABLE_NAME,null, whereClause, whereArgs, null, null, null);
			}
		}

		while(cursor.moveToNext()){
			String row = cursor.getString(0)+"@"+cursor.getString(1)+"@"+cursor.getString(2)
					+"@"+cursor.getString(3)+"#";
			sendKeyValueQuery.append(row);
		}
		Log.v("HANDLE_QUERY4",sendKeyValueQuery.toString());
		return cursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		String str = "key = '"+selection+"'";
		SQLiteDatabase db = dbHelper.getWritableDatabase();
		int count = db.update(TABLE_NAME, values, str, selectionArgs);
        Log.v("update Row: ", count+"");
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
					}else if(reqType.equalsIgnoreCase("QUERY_KEY") || reqType.equalsIgnoreCase("QUERY_SEND_KEY")){
                        String key = rect[1];
                        String source = rect[2];
						signalQuery3 = false;
                        publishProgress("QUERY_KEY",key,source);
                        while(!signalQuery3){}
                        Log.v("SERVER_QUERY_KEY",sendKeyValueQuery+"****");
                        String replyToClient = "QUERY_KEY"+"~"+sendKeyValueQuery;
                        DataOutputStream output = new DataOutputStream(sc.getOutputStream());
                        output.writeUTF(replyToClient);
                    }else if(reqType.equalsIgnoreCase("QUERY_ALL")){
						String key = rect[1];
						String source = rect[2];
						signalQuery3 = false;
						publishProgress("QUERY_KEY",key,"*");
						while(!signalQuery3){}
						Log.v("SERVER_QUERY_ALL",sendKeyValueQuery+"****");
						String replyToClient = "QUERY_KEY"+"~"+sendKeyValueQuery;
						DataOutputStream output = new DataOutputStream(sc.getOutputStream());
						output.writeUTF(replyToClient);
					}else if(reqType.equalsIgnoreCase("DELETE_KEY")){
						String key = rect[1];
						signalDelete2 = false;
						publishProgress("DELETE_KEY",key);
						while(!signalDelete2){}
						Log.v("SERVER_QUERY_ALL",key+"****");
						String replyToClient = "DELETE_KEY";
						DataOutputStream output = new DataOutputStream(sc.getOutputStream());
						output.writeUTF(replyToClient);
					}else if(reqType.equalsIgnoreCase("QUERY_ALL_INIT")){
						String key = rect[1];
						String source = rect[2];
						signalInit2 = false;
						publishProgress("QUERY_ALL_INIT",key,source);
						while(!signalInit2){}
						Log.v("SERVER_QUERY_ALL_INIT",sendKeyValueQuery+"****");
						String replyToClient = "QUERY_ALL_INIT"+"~"+sendKeyValueQuery;
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
			}else if(param.equalsIgnoreCase("QUERY_KEY")){
				signalQuery3 = false;
				String key = strings[1];
				String source = strings[2];
				Log.v("QUERY_KEY_Other1",key+"~"+source);
				Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				handleQuery(mUri, key,source);
				Log.v("QUERY_KEY_Other2",key+"~"+source+"~"+sendKeyValueQuery);
				signalQuery3 = true;
			}else if(param.equalsIgnoreCase("DELETE_KEY")){
				signalDelete2 = false;
				String key = strings[1];
				Log.v("DELETE_KEY_Other1",key);
				Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				handleDelete(mUri, key);
				Log.v("DELETE_KEY_Other2",key);
				signalDelete2 = true;
			}else if(param.equalsIgnoreCase("QUERY_ALL_INIT")){
				signalInit2 = false;
				String key = strings[1];
				String source = strings[2];
				Log.v("QUERY_INIT_Other1",key+"~"+source);
				Uri mUri = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");
				handleQuery(mUri, key,source);
				Log.v("QUERY_INIT_Other2",key+"~"+source+"~"+sendKeyValueQuery);
				signalInit2 = true;
			}

			return null;
		}
	}

	class OtherClientTask extends AsyncTask<String, Void, Void>{

		@Override
		protected Void doInBackground(String... msgs) {
			String msg = msgs[0];
			DataInputStream ackRec;
			DataOutputStream output;

			if(msg.equalsIgnoreCase("QUERY_ALL_INIT") ){
				signalInit1 = false;
				receivedKeyValueQuery = new StringBuilder();
				String key = msgs[1];
				String source = msgs[2];
				String port1 = msgs[3];
				String port2 = msgs[4];

				for(String p : REMOTE_PORT){
					if(!p.equals(myPort)){
						try{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(p));
							socket.setSoTimeout(5000);
							String msgToSend ="";
							if(p.equals(port1) || p.equals(port2))
								msgToSend = msg+"~"+key+"~"+source;
							else
								msgToSend = msg+"~"+key+"~"+p;

							Log.v("Client_Request_INIT:",msgToSend+"~"+p);
							output = new DataOutputStream(socket.getOutputStream());
							output.writeUTF(msgToSend);

							ackRec = new DataInputStream(socket.getInputStream());
							String ackStr = ackRec.readUTF();
							Log.v("Client_Ack_INIT:",ackStr+"~"+p);
							String[] receivedInfo = ackStr.split("~");

							if(receivedInfo[0].equals("QUERY_ALL_INIT")){
								if(receivedInfo.length > 1){
									receivedKeyValueQuery.append(receivedInfo[1]);
								}

								output.close();
								ackRec.close();
								socket.close();
							}
						}catch(SocketTimeoutException e){
							Log.e(TAG, "ClientOtherTask socket SocketTimeoutException");
						}catch(IOException e){
							Log.e(TAG, "ClientOtherTask socket IOException");
						}catch(Exception e){
							e.printStackTrace();
							Log.e(TAG, "ClientOtherTask Exception OtherTask");
						}
					}
				}
				Log.v("QUERY_ALL_INIT_KEYS",receivedKeyValueQuery+":");
				signalInit1 = true;
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
					String source = msgs[2];
					ArrayList<String> port = new ArrayList<String>();
					if(msg.equalsIgnoreCase("QUERY_KEY")){
						port.add(msgs[3]);
						port.add(msgs[4]);
					}else{
						port.add(msgs[2]);
						port.add(msgs[3]);
						port.add(msgs[4]);
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
								if(receivedInfo.length > 1)
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
					signalQuery1 = true;
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
                    insertKey = true;
				}else if(msg.equalsIgnoreCase("QUERY_ALL")){
					signalQuery1 = false;
					receivedKeyValueQuery = new StringBuilder();
					String key = msgs[1];
					String source = msgs[2];

					for(String p : REMOTE_PORT){
						if(!p.equals(myPort)){
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
									if(receivedInfo.length > 1)
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
					}
					signalQuery1 = true;
				}else if(msg.equalsIgnoreCase("DELETE_KEY") || msg.equalsIgnoreCase("DELETE_ALL")){
					signalDelete1 = false;
					String key = msgs[1];
					ArrayList<String> port = new ArrayList<String>();
					if(!msg.equalsIgnoreCase("DELETE_ALL")){
						String source = msgs[2];
						if(source.equals(myPort)){
							port.add(msgs[3]);
							port.add(msgs[4]);
						}else{
							port.add(msgs[2]);
							port.add(msgs[3]);
							port.add(msgs[4]);
						}
					}else{
						for(String p: REMOTE_PORT){
							if(!p.equals(myPort))
								port.add(p);
						}
					}


					for(String p : port){
						try{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
									Integer.parseInt(p));
							socket.setSoTimeout(5000);
							String msgToSend = msg+"~"+key;
							Log.v("Client Request:",msgToSend+"~"+p);
							output = new DataOutputStream(socket.getOutputStream());
							output.writeUTF(msgToSend);

							ackRec = new DataInputStream(socket.getInputStream());
							String ackStr = ackRec.readUTF();
							Log.v("Client Ack from Server:",ackStr+"~"+p);
							String[] receivedInfo = ackStr.split("~");

							if(receivedInfo[0].equals("DELETE_KEY")){
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
					signalDelete1 = true;
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

	private String printRing() {
		String test="";
		for(Map.Entry<String, Node> node : ring.entrySet()){
//                test += node.getValue().key+" "+node.getValue().pred.getKey()+" "+node.getValue().succ.key+"^^";
			test += node.getValue().key+" "+node.getValue().succ.key+"^^";
		}
		return test;
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
			Log.v("ON_CREATE1","!!!!!!!!!!!!!!!");
		}
		@Override
		public void onCreate(SQLiteDatabase db) {
			try{
				tableExist =false;
				Log.v("ON_CREATE2","!!!!!!!!!!!!!!!");
				db.execSQL(SQL_CREATE_ENTRIES);
			}catch (Exception e){
				e.printStackTrace();
			}
		}
		public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onCreate(db);
		}
		public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
			onUpgrade(db, oldVersion, newVersion);
		}

		public boolean checkTable(SQLiteDatabase db) {
			String str = "select DISTINCT tbl_name from sqlite_master";
			Cursor cursor = db.rawQuery(str,null);
			Log.v("ON_CREATE2",cursor.getCount()+"!");
			while(cursor.moveToNext()){
				Log.v("ON_CREATE_03","EXIST! "+cursor.getString(0));
				if(cursor.getString(0).equals("dynamo")){
					tableExist = true;
					Log.v("ON_CREATE3","EXIST! EXIST!");
				}
			}

			return tableExist;
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
