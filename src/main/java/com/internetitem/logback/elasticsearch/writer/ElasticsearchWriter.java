package com.internetitem.logback.elasticsearch.writer;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.util.Collection;
import java.util.Collections;

import com.internetitem.logback.elasticsearch.config.HttpRequestHeader;
import com.internetitem.logback.elasticsearch.config.HttpRequestHeaders;
import com.internetitem.logback.elasticsearch.config.Settings;
import com.internetitem.logback.elasticsearch.util.ErrorReporter;

public class ElasticsearchWriter implements SafeWriter {

	private StringBuilder backupBuffer;
	private StringBuilder sendBuffer;

	private ErrorReporter errorReporter;
	private Settings settings;
	private Collection<HttpRequestHeader> headerList;

	private boolean bufferExceeded;
	private boolean backupBufferExceeded;

	public ElasticsearchWriter(ErrorReporter errorReporter, Settings settings, HttpRequestHeaders headers) {
		this.errorReporter = errorReporter;
		this.settings = settings;
		this.headerList = headers != null && headers.getHeaders() != null
			? headers.getHeaders()
			: Collections.<HttpRequestHeader>emptyList();

		this.sendBuffer = new StringBuilder();
		this.backupBuffer = new StringBuilder();
	}

	public void write(char[] cbuf, int off, int len) {
		if (!bufferExceeded && backupBuffer.length() > 0) {
			errorReporter.logWarning("Illegal state");
		}
		if (bufferExceeded) {
			if (backupBuffer.length() + len <= settings.getMaxQueueSize()) {
				backupBuffer.append(cbuf, off, len);
			} else if (!backupBufferExceeded) {
				backupBufferExceeded = true;
				errorReporter.logWarning("Backup queue maximum size exceeded - log messages will be dropped until successful request");
			}
			return;
		}

		if (sendBuffer.length() + len > settings.getMaxQueueSize()) {
			if (!bufferExceeded) {
				errorReporter.logWarning("Send queue maximum size exceeded - log messages will be collected in memory for now");
			}
			bufferExceeded = true;
			if (backupBuffer.length() + len <= settings.getMaxQueueSize()) {
				backupBuffer.append(cbuf, off, len);
			} else if (!backupBufferExceeded) {
				backupBufferExceeded = true;
				errorReporter.logWarning("Backup queue maximum size exceeded - log messages will be dropped until successful request");
			}
			return;
		}

		sendBuffer.append(cbuf, off, len);
	}

	public void sendData() throws IOException {
		if (sendBuffer.length() <= 0) {
			return;
		}

		HttpURLConnection urlConnection = (HttpURLConnection)(settings.getUrl().openConnection());
		try {
			urlConnection.setDoInput(true);
			urlConnection.setDoOutput(true);
			urlConnection.setReadTimeout(settings.getReadTimeout());
			urlConnection.setConnectTimeout(settings.getConnectTimeout());
			urlConnection.setRequestMethod("POST");

			String body = sendBuffer.toString();

			if (!headerList.isEmpty()) {
				for(HttpRequestHeader header: headerList) {
					urlConnection.setRequestProperty(header.getName(), header.getValue());
				}
			}

			if (settings.getAuthentication() != null) {
				settings.getAuthentication().addAuth(urlConnection, body);
			}

			Writer writer = new OutputStreamWriter(urlConnection.getOutputStream(), "UTF-8");
			writer.write(body);
			writer.flush();
			writer.close();

			int rc = urlConnection.getResponseCode();
			if (rc != 200) {
				String data = slurpErrors(urlConnection);
				throw new IOException("Got response code [" + rc + "] from server with data " + data);
			}
		} finally {
			urlConnection.disconnect();
		}

		updateBuffersOnSuccess();
	}

	public void clear() {
		updateBuffersOnSuccess();
	}

	private void updateBuffersOnSuccess() {
		sendBuffer.setLength(0);
		if (backupBuffer.length() > 0) {
			sendBuffer.append(backupBuffer.toString());
			errorReporter.logInfo("Backup queue cleared. " + backupBuffer.length() + " bytes moved to Send queue");
			backupBuffer.setLength(0);
			backupBufferExceeded = false;
		}
		if (bufferExceeded && sendBuffer.length() < settings.getMaxQueueSize()) {
			errorReporter.logInfo("Send queue back in bounds (" + sendBuffer.length() + ") - log messages will no longer be lost or accumulated in memory");
			bufferExceeded = false;
		}
	}

	public boolean hasPendingData() {
		return sendBuffer.length() != 0 || backupBuffer.length() != 0;
	}

	private static String slurpErrors(HttpURLConnection urlConnection) {
		try {
			InputStream stream = urlConnection.getErrorStream();
			if (stream == null) {
				return "<no data>";
			}

			StringBuilder builder = new StringBuilder();
			InputStreamReader reader = new InputStreamReader(stream, "UTF-8");
			char[] buf = new char[2048];
			int numRead;
			while ((numRead = reader.read(buf)) > 0) {
				builder.append(buf, 0, numRead);
			}
			return builder.toString();
		} catch (Exception e) {
			return "<error retrieving data: " + e.getMessage() + ">";
		}
	}

}
