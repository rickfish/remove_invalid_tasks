package com.bcbsfl.es;

import java.io.PrintWriter;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JsonUtils {
	static final JsonParser jsonParser = new JsonParser();
	static public void convertMapStringToJsonObject(PrintWriter parseErrorFileWriter, String id, String mainAttrName, JsonObject jo, String mapString) throws Exception {
		int startingPointOfSearch = 0;
		int equalsSign = mapString.indexOf("=", startingPointOfSearch);
		while (equalsSign != -1) {
			int prevLeftParen = mapString.lastIndexOf("{", equalsSign);
			int prevComma = mapString.lastIndexOf(",", equalsSign);
			String attrName = null, attrValue = null;
			if (prevComma != -1 || prevLeftParen != -1) {
				attrName = mapString.substring((prevLeftParen > prevComma ? prevLeftParen + 1 : prevComma + 1),
						equalsSign);
			} else {
				break; // This should never happen
			}
			if (equalsSign == mapString.length() - 1) { // last property is an empty value
				jo.addProperty(attrName, "");
				break;
			} else {
				int endOfJsonObject = -1;
				JsonObject nestedObj = null;
				JsonArray nestedArray = null;
				int nextChar = equalsSign + 1;
				switch (mapString.charAt(nextChar)) {
				case '}': // last property is the last one in a Json object
					jo.addProperty(attrName, "");
					startingPointOfSearch = equalsSign + 1;
					break;
				case '{': // this property is a Json object
					endOfJsonObject = findEndOfJsonObject(mapString, nextChar);
					nestedObj = new JsonObject();
					jo.add(attrName, nestedObj);
					convertMapStringToJsonObject(parseErrorFileWriter, id, mainAttrName, nestedObj, mapString.substring(nextChar, endOfJsonObject + 1));
					startingPointOfSearch = endOfJsonObject;
					break;
				case '[': // this property is a Json array
					endOfJsonObject = findEndOfArray(mapString, nextChar);
					String arrayString = mapString.substring(nextChar, endOfJsonObject + 1);
					if (arrayString.indexOf("{") == -1) { // Must be a string array if no inner Json objects
						try {
							JsonElement jsonArray = new JsonParser().parse(arrayString);
							jo.add(attrName, jsonArray);
						} catch(Exception e) {
							jo.add(attrName, jsonParser.parse("[\"ES5 to ES7 Migration Error: Could not parse what looked to be a JSON array but was probably not\"]"));
							parseErrorFileWriter.println("[ID: " + id + ", Outer Attribute: " + mainAttrName + ", Attribute: " + attrName + "] " + e.getMessage());
							parseErrorFileWriter.println("\t" + arrayString);
							parseErrorFileWriter.flush();
							System.err.println("[ID: " + id + ", Outer Attribute: " + mainAttrName + ", Attribute: " + attrName + "] " + e.getMessage());
						}
					} else {
						nestedArray = new JsonArray();
						jo.add(attrName, nestedArray);
						convertArrayStringToJsonArray(parseErrorFileWriter, id, mainAttrName, nestedArray, attrName, arrayString);
					}
					startingPointOfSearch = endOfJsonObject;
					break;
				default: // this property is a normal value
					int nextComma = mapString.indexOf(",", nextChar);
					int nextRightParen = mapString.indexOf("}", nextChar);
					int endOfProp = (nextComma != -1 && nextComma < nextRightParen ? nextComma : nextRightParen);
					attrValue = mapString.substring(equalsSign + 1, endOfProp);
					jo.addProperty(attrName, attrValue.trim());
					startingPointOfSearch = endOfProp;
					break;
				}
			}
			equalsSign = mapString.indexOf("=", startingPointOfSearch);
		}
	}

	static private void convertArrayStringToJsonArray(PrintWriter parseErrorFileWriter, String id, String mainAttrName, JsonArray ja, String attrName, String arrayString) throws Exception {
		if (arrayString.length() < 3) { // only contains the [ and ] characters (empty array)
			return;
		}
		JsonObject jo = null;
		int rightParen = -1;
		int leftParen = arrayString.indexOf("{", 0);
		while (leftParen != -1) {
			rightParen = findEndOfJsonObject(arrayString, leftParen);
			jo = new JsonObject();
			ja.add(jo);
			convertMapStringToJsonObject(parseErrorFileWriter, id, mainAttrName, jo, arrayString.substring(leftParen, rightParen + 1));
			leftParen = arrayString.indexOf("{", rightParen + 1);
		}
	}

	static private int findEndOfArray(String s, int startOfArray) {
		return findEndOfObject(s, startOfArray, '[', ']');
	}

	static private int findEndOfJsonObject(String s, int startOfArray) {
		return findEndOfObject(s, startOfArray, '{', '}');
	}

	static private int findEndOfObject(String s, int startOfObject, char startOfObjectChar, char endOfObjectChar) {
		int stringLength = s.length();
		int endOfObject = stringLength - 1;
		char currentChar = ' ';
		for (int i = startOfObject + 1; i < stringLength; i++) {
			currentChar = s.charAt(i);
			if (currentChar == '{') {
				i = findEndOfJsonObject(s, i) + 1;
			} else if (currentChar == '[') {
				i = findEndOfArray(s, i) + 1;
			} else {
				if (currentChar == endOfObjectChar) {
					endOfObject = i;
					break;
				}
			}
		}
		return endOfObject;
	}
}
