package com.yy.jdbc.proxy.driver;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;

public class StrUtil
{
  private static final int BYTE_RANGE = 256;
  private static byte[] allBytes = new byte[256];

  private static char[] byteToChars = new char[256];
  private static Method toPlainStringMethod;
  static final int WILD_COMPARE_MATCH_NO_WILD = 0;
  static final int WILD_COMPARE_MATCH_WITH_WILD = 1;
  static final int WILD_COMPARE_NO_MATCH = -1;
  private static final ConcurrentHashMap<String, Charset> charsetsByAlias = new ConcurrentHashMap();

  private static final String platformEncoding = System.getProperty("file.encoding");
  private static final String VALID_ID_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ0123456789$_#@";

  static Charset findCharset(String alias)
    throws UnsupportedEncodingException
  {
    try
    {
      Charset cs = (Charset)charsetsByAlias.get(alias);

      if (cs == null) {
        cs = Charset.forName(alias);
        charsetsByAlias.putIfAbsent(alias, cs);
      }

      return cs;
    }
    catch (UnsupportedCharsetException uce)
    {
      throw new UnsupportedEncodingException(alias);
    } catch (IllegalCharsetNameException icne) {
      throw new UnsupportedEncodingException(alias);
    } catch (IllegalArgumentException iae) {
      throw new UnsupportedEncodingException(alias);
    }
  }

  public static String consistentToString(BigDecimal decimal)
  {
    if (decimal == null) {
      return null;
    }

    if (toPlainStringMethod != null)
      try {
        return ((String)toPlainStringMethod.invoke(decimal, (Object[])null));
      }
      catch (InvocationTargetException invokeEx)
      {
      }
      catch (IllegalAccessException accessEx)
      {
      }
    return decimal.toString();
  }

  public static final String dumpAsHex(byte[] byteBuffer, int length)
  {
    StringBuffer outputBuf = new StringBuffer(length * 4);

    int p = 0;
    int rows = length / 8;

    for (int i = 0; (i < rows) && (p < length); ++i) {
      int ptemp = p;

      for (int j = 0; j < 8; ++j) {
        String hexVal = Integer.toHexString(byteBuffer[ptemp] & 0xFF);

        if (hexVal.length() == 1) {
          hexVal = "0" + hexVal;
        }

        outputBuf.append(hexVal + " ");
        ++ptemp;
      }

      outputBuf.append("    ");

      for (int j = 0; j < 8; ++j) {
        int b = 0xFF & byteBuffer[p];

        if ((b > 32) && (b < 127))
          outputBuf.append((char)b + " ");
        else {
          outputBuf.append(". ");
        }

        ++p;
      }

      outputBuf.append("\n");
    }

    int n = 0;

    for (int i = p; i < length; ++i) {
      String hexVal = Integer.toHexString(byteBuffer[i] & 0xFF);

      if (hexVal.length() == 1) {
        hexVal = "0" + hexVal;
      }

      outputBuf.append(hexVal + " ");
      ++n;
    }

    for (int i = n; i < 8; ++i) {
      outputBuf.append("   ");
    }

    outputBuf.append("    ");

    for (int i = p; i < length; ++i) {
      int b = 0xFF & byteBuffer[i];

      if ((b > 32) && (b < 127))
        outputBuf.append((char)b + " ");
      else {
        outputBuf.append(". ");
      }
    }

    outputBuf.append("\n");

    return outputBuf.toString();
  }

  private static boolean endsWith(byte[] dataFrom, String suffix) {
    for (int i = 1; i <= suffix.length(); ++i) {
      int dfOffset = dataFrom.length - i;
      int suffixOffset = suffix.length() - i;
      if (dataFrom[dfOffset] != suffix.charAt(suffixOffset)) {
        return false;
      }
    }
    return true;
  }

  public static byte[] escapeEasternUnicodeByteStream(byte[] origBytes, String origString, int offset, int length)
  {
    if ((origBytes == null) || (origBytes.length == 0)) {
      return origBytes;
    }

    int bytesLen = origBytes.length;
    int bufIndex = 0;
    int strIndex = 0;

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream(bytesLen);
    while (true)
    {
      if (origString.charAt(strIndex) == '\\')
      {
        bytesOut.write(origBytes[(bufIndex++)]);
      }
      else
      {
        int loByte = origBytes[bufIndex];

        if (loByte < 0) {
          loByte += 256;
        }

        bytesOut.write(loByte);

        if (loByte >= 128) {
          if (bufIndex < bytesLen - 1) {
            int hiByte = origBytes[(bufIndex + 1)];

            if (hiByte < 0) {
              hiByte += 256;
            }

            bytesOut.write(hiByte);
            ++bufIndex;

            if (hiByte == 92)
              bytesOut.write(hiByte);
          }
        }
        else if ((loByte == 92) && 
          (bufIndex < bytesLen - 1)) {
          int hiByte = origBytes[(bufIndex + 1)];

          if (hiByte < 0) {
            hiByte += 256;
          }

          if (hiByte == 98)
          {
            bytesOut.write(92);
            bytesOut.write(98);
            ++bufIndex;
          }

        }

        ++bufIndex;
      }

      if (bufIndex >= bytesLen)
      {
        break;
      }

      ++strIndex;
    }

    return bytesOut.toByteArray();
  }

  public static char firstNonWsCharUc(String searchIn)
  {
    return firstNonWsCharUc(searchIn, 0);
  }

  public static char firstNonWsCharUc(String searchIn, int startAt) {
    if (searchIn == null) {
      return '\0';
    }

    int length = searchIn.length();

    for (int i = startAt; i < length; ++i) {
      char c = searchIn.charAt(i);

      if (!(Character.isWhitespace(c))) {
        return Character.toUpperCase(c);
      }
    }

    return '\0';
  }

  public static char firstAlphaCharUc(String searchIn, int startAt) {
    if (searchIn == null) {
      return '\0';
    }

    int length = searchIn.length();

    for (int i = startAt; i < length; ++i) {
      char c = searchIn.charAt(i);

      if (Character.isLetter(c)) {
        return Character.toUpperCase(c);
      }
    }

    return '\0';
  }

  public static final String fixDecimalExponent(String dString)
  {
    int ePos = dString.indexOf("E");

    if (ePos == -1) {
      ePos = dString.indexOf("e");
    }

    if ((ePos != -1) && 
      (dString.length() > ePos + 1)) {
      char maybeMinusChar = dString.charAt(ePos + 1);

      if ((maybeMinusChar != '-') && (maybeMinusChar != '+')) {
        StringBuffer buf = new StringBuffer(dString.length() + 1);
        buf.append(dString.substring(0, ePos + 1));
        buf.append('+');
        buf.append(dString.substring(ePos + 1, dString.length()));
        dString = buf.toString();
      }

    }

    return dString;
  }



  public static int getInt(byte[] buf, int offset, int endPos)
    throws NumberFormatException
  {
    int base = 10;

    int s = offset;

    while ((Character.isWhitespace((char)buf[s])) && (s < endPos)) {
      ++s;
    }

    if (s == endPos) {
      throw new NumberFormatException(toString(buf));
    }

    boolean negative = false;

    if ((char)buf[s] == '-') {
      negative = true;
      ++s;
    } else if ((char)buf[s] == '+') {
      ++s;
    }

    int save = s;

    int cutoff = 2147483647 / base;
    int cutlim = 2147483647 % base;

    if (negative) {
      ++cutlim;
    }

    boolean overflow = false;

    int i = 0;

    for (; s < endPos; ++s) {
      char c = (char)buf[s];

      if (Character.isDigit(c)) {
        c = (char)(c - '0'); } else {
        if (!(Character.isLetter(c))) break;
        c = (char)(Character.toUpperCase(c) - 'A' + 10);
      }

      if (c >= base)
      {
        break;
      }

      if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
        overflow = true;
      } else {
        i *= base;
        i += c;
      }
    }

    if (s == save) {
      throw new NumberFormatException(toString(buf));
    }

    if (overflow) {
      throw new NumberFormatException(toString(buf));
    }

    return ((negative) ? -i : i);
  }

  public static int getInt(byte[] buf) throws NumberFormatException {
    return getInt(buf, 0, buf.length);
  }

  public static long getLong(byte[] buf) throws NumberFormatException {
    return getLong(buf, 0, buf.length);
  }

  public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
    int base = 10;

    int s = offset;

    while ((Character.isWhitespace((char)buf[s])) && (s < endpos)) {
      ++s;
    }

    if (s == endpos) {
      throw new NumberFormatException(toString(buf));
    }

    boolean negative = false;

    if ((char)buf[s] == '-') {
      negative = true;
      ++s;
    } else if ((char)buf[s] == '+') {
      ++s;
    }

    int save = s;

    long cutoff = 9223372036854775807L / base;
    long cutlim = (int)(9223372036854775807L % base);

    if (negative) {
      cutlim += 1L;
    }

    boolean overflow = false;
    long i = 0L;

    for (; s < endpos; ++s) {
      char c = (char)buf[s];

      if (Character.isDigit(c)) {
        c = (char)(c - '0'); } else {
        if (!(Character.isLetter(c))) break;
        c = (char)(Character.toUpperCase(c) - 'A' + 10);
      }

      if (c >= base)
      {
        break;
      }

      if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
        overflow = true;
      } else {
        i *= base;
        i += c;
      }
    }

    if (s == save) {
      throw new NumberFormatException(toString(buf));
    }

    if (overflow) {
      throw new NumberFormatException(toString(buf));
    }

    return ((negative) ? -i : i);
  }

  public static short getShort(byte[] buf) throws NumberFormatException {
    short base = 10;

    int s = 0;

    while ((Character.isWhitespace((char)buf[s])) && (s < buf.length)) {
      ++s;
    }

    if (s == buf.length) {
      throw new NumberFormatException(toString(buf));
    }

    boolean negative = false;

    if ((char)buf[s] == '-') {
      negative = true;
      ++s;
    } else if ((char)buf[s] == '+') {
      ++s;
    }

    int save = s;

    short cutoff = (short)(32767 / base);
    short cutlim = (short)(32767 % base);

    if (negative) {
      cutlim = (short)(cutlim + 1);
    }

    boolean overflow = false;
    short i = 0;

    for (; s < buf.length; ++s) {
      char c = (char)buf[s];

      if (Character.isDigit(c)) {
        c = (char)(c - '0'); } else {
        if (!(Character.isLetter(c))) break;
        c = (char)(Character.toUpperCase(c) - 'A' + 10);
      }

      if (c >= base)
      {
        break;
      }

      if ((i > cutoff) || ((i == cutoff) && (c > cutlim))) {
        overflow = true;
      } else {
        i = (short)(i * base);
        i = (short)(i + c);
      }
    }

    if (s == save) {
      throw new NumberFormatException(toString(buf));
    }

    if (overflow) {
      throw new NumberFormatException(toString(buf));
    }

    return ((negative) ? (short)(-i) : i);
  }

  public static final int indexOfIgnoreCase(int startingPosition, String searchIn, String searchFor)
  {
    if ((searchIn == null) || (searchFor == null) || (startingPosition > searchIn.length()))
    {
      return -1;
    }

    int patternLength = searchFor.length();
    int stringLength = searchIn.length();
    int stopSearchingAt = stringLength - patternLength;

    if (patternLength == 0) {
      return -1;
    }

    char firstCharOfPatternUc = Character.toUpperCase(searchFor.charAt(0));
    char firstCharOfPatternLc = Character.toLowerCase(searchFor.charAt(0));

    for (int i = startingPosition; i <= stopSearchingAt; ++i) {
      if ((!(isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc, firstCharOfPatternLc, i))) || 
        (++i > stopSearchingAt) || (!(isNotEqualIgnoreCharCase(searchIn, firstCharOfPatternUc, firstCharOfPatternLc, i))));
      if (i > stopSearchingAt) {
        continue;
      }
      int j = i + 1;
      int end = j + patternLength - 1;
      for (int k = 1; (j < end) && (((Character.toLowerCase(searchIn.charAt(j)) == Character.toLowerCase(searchFor.charAt(k))) || (Character.toUpperCase(searchIn.charAt(j)) == Character.toUpperCase(searchFor.charAt(k))))); )
      {
        ++j; ++k;
      }
      if (j == end) {
        return i;
      }

    }

    return -1;
  }

  private static final boolean isNotEqualIgnoreCharCase(String searchIn, char firstCharOfPatternUc, char firstCharOfPatternLc, int i)
  {
    return ((Character.toLowerCase(searchIn.charAt(i)) != firstCharOfPatternLc) && (Character.toUpperCase(searchIn.charAt(i)) != firstCharOfPatternUc));
  }

  public static final int indexOfIgnoreCase(String searchIn, String searchFor)
  {
    return indexOfIgnoreCase(0, searchIn, searchFor);
  }

  public static int indexOfIgnoreCaseRespectMarker(int startAt, String src, String target, String marker, String markerCloses, boolean allowBackslashEscapes)
  {
    char contextMarker = '\0';
    boolean escaped = false;
    int markerTypeFound = 0;
    int srcLength = src.length();
    int ind = 0;

    for (int i = startAt; i < srcLength; ++i) {
      char c = src.charAt(i);

      if ((allowBackslashEscapes) && (c == '\\')) {
        escaped = !(escaped);
      } else if ((contextMarker != 0) && (c == markerCloses.charAt(markerTypeFound)) && (!(escaped))) {
        contextMarker = '\0';
      } else if (((ind = marker.indexOf(c)) != -1) && (!(escaped)) && (contextMarker == 0))
      {
        markerTypeFound = ind;
        contextMarker = c;
      } else if ((((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0))) || (Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))))) && (!(escaped)) && (contextMarker == 0) && 
        (startsWithIgnoreCase(src, i, target))) {
        return i;
      }
    }

    return -1;
  }

  public static int indexOfIgnoreCaseRespectQuotes(int startAt, String src, String target, char quoteChar, boolean allowBackslashEscapes)
  {
    char contextMarker = '\0';
    boolean escaped = false;

    int srcLength = src.length();

    for (int i = startAt; i < srcLength; ++i) {
      char c = src.charAt(i);

      if ((allowBackslashEscapes) && (c == '\\'))
        escaped = !(escaped);
      else if ((c == contextMarker) && (!(escaped)))
        contextMarker = '\0';
      else if ((c == quoteChar) && (!(escaped)) && (contextMarker == 0))
      {
        contextMarker = c;
      }
      else if ((((Character.toUpperCase(c) == Character.toUpperCase(target.charAt(0))) || (Character.toLowerCase(c) == Character.toLowerCase(target.charAt(0))))) && (!(escaped)) && (contextMarker == 0) && 
        (startsWithIgnoreCase(src, i, target))) {
        return i;
      }
    }

    return -1;
  }

  public static final List<String> split(String stringToSplit, String delimitter, boolean trim)
  {
    if (stringToSplit == null) {
      return new ArrayList();
    }

    if (delimitter == null) {
      throw new IllegalArgumentException();
    }

    StringTokenizer tokenizer = new StringTokenizer(stringToSplit, delimitter, false);

    List splitTokens = new ArrayList(tokenizer.countTokens());

    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();

      if (trim) {
        token = token.trim();
      }

      splitTokens.add(token);
    }

    return splitTokens;
  }

  public static final List<String> split(String stringToSplit, String delimiter, String markers, String markerCloses, boolean trim)
  {
    if (stringToSplit == null) {
      return new ArrayList();
    }

    if (delimiter == null) {
      throw new IllegalArgumentException();
    }

    int delimPos = 0;
    int currentPos = 0;

    List splitTokens = new ArrayList();

    while ((delimPos = indexOfIgnoreCaseRespectMarker(currentPos, stringToSplit, delimiter, markers, markerCloses, false)) != -1) {
      String token = stringToSplit.substring(currentPos, delimPos);

      if (trim) {
        token = token.trim();
      }

      splitTokens.add(token);
      currentPos = delimPos + 1;
    }

    if (currentPos < stringToSplit.length()) {
      String token = stringToSplit.substring(currentPos);

      if (trim) {
        token = token.trim();
      }

      splitTokens.add(token);
    }

    return splitTokens;
  }

  private static boolean startsWith(byte[] dataFrom, String chars) {
    for (int i = 0; i < chars.length(); ++i) {
      if (dataFrom[i] != chars.charAt(i)) {
        return false;
      }
    }
    return true;
  }

  public static boolean startsWithIgnoreCase(String searchIn, int startAt, String searchFor)
  {
    return searchIn.regionMatches(true, startAt, searchFor, 0, searchFor.length());
  }

  public static boolean startsWithIgnoreCase(String searchIn, String searchFor)
  {
    return startsWithIgnoreCase(searchIn, 0, searchFor);
  }

  public static boolean startsWithIgnoreCaseAndNonAlphaNumeric(String searchIn, String searchFor)
  {
    if (searchIn == null) {
      return (searchFor == null);
    }

    int beginPos = 0;

    int inLength = searchIn.length();

    for (beginPos = 0; beginPos < inLength; ++beginPos) {
      char c = searchIn.charAt(beginPos);

      if (Character.isLetterOrDigit(c)) {
        break;
      }
    }

    return startsWithIgnoreCase(searchIn, beginPos, searchFor);
  }

  public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor)
  {
    return startsWithIgnoreCaseAndWs(searchIn, searchFor, 0);
  }

  public static boolean startsWithIgnoreCaseAndWs(String searchIn, String searchFor, int beginPos)
  {
    if (searchIn == null) {
      return (searchFor == null);
    }

    int inLength = searchIn.length();

    for (; beginPos < inLength; ++beginPos) {
      if (!(Character.isWhitespace(searchIn.charAt(beginPos)))) {
        break;
      }
    }

    return startsWithIgnoreCase(searchIn, beginPos, searchFor);
  }

  public static byte[] stripEnclosure(byte[] source, String prefix, String suffix)
  {
    if ((source.length >= prefix.length() + suffix.length()) && (startsWith(source, prefix)) && (endsWith(source, suffix)))
    {
      int totalToStrip = prefix.length() + suffix.length();
      int enclosedLength = source.length - totalToStrip;
      byte[] enclosed = new byte[enclosedLength];

      int startPos = prefix.length();
      int numToCopy = enclosed.length;
      System.arraycopy(source, startPos, enclosed, 0, numToCopy);

      return enclosed;
    }
    return source;
  }

  public static final String toAsciiString(byte[] buffer)
  {
    return toAsciiString(buffer, 0, buffer.length);
  }

  public static final String toAsciiString(byte[] buffer, int startPos, int length)
  {
    char[] charArray = new char[length];
    int readpoint = startPos;

    for (int i = 0; i < length; ++i) {
      charArray[i] = (char)buffer[readpoint];
      ++readpoint;
    }

    return new String(charArray);
  }

  public static int wildCompare(String searchIn, String searchForWildcard)
  {
    if ((searchIn == null) || (searchForWildcard == null)) {
      return -1;
    }

    if (searchForWildcard.equals("%"))
    {
      return 1;
    }

    int result = -1;

    char wildcardMany = '%';
    char wildcardOne = '_';
    char wildcardEscape = '\\';

    int searchForPos = 0;
    int searchForEnd = searchForWildcard.length();

    int searchInPos = 0;
    int searchInEnd = searchIn.length();

    while (searchForPos != searchForEnd) {
      char wildstrChar = searchForWildcard.charAt(searchForPos);

      while ((searchForWildcard.charAt(searchForPos) != wildcardMany) && (wildstrChar != wildcardOne)) {
        if ((searchForWildcard.charAt(searchForPos) == wildcardEscape) && (searchForPos + 1 != searchForEnd))
        {
          ++searchForPos;
        }

        if ((searchInPos == searchInEnd) || (Character.toUpperCase(searchForWildcard.charAt(searchForPos++)) != Character.toUpperCase(searchIn.charAt(searchInPos++))))
        {
          return 1;
        }

        if (searchForPos == searchForEnd) {
          return ((searchInPos != searchInEnd) ? 1 : 0);
        }

        result = 1;
      }

      if (searchForWildcard.charAt(searchForPos) == wildcardOne) {
        do {
          if (searchInPos == searchInEnd)
          {
            return result;
          }

          ++searchInPos;
        }
        while ((++searchForPos < searchForEnd) && (searchForWildcard.charAt(searchForPos) == wildcardOne));

        if (searchForPos == searchForEnd) {
          break;
        }
      }

      if (searchForWildcard.charAt(searchForPos) == wildcardMany)
      {
        ++searchForPos;

        for (; searchForPos != searchForEnd; ++searchForPos) {
          if (searchForWildcard.charAt(searchForPos) == wildcardMany) {
            continue;
          }

          if (searchForWildcard.charAt(searchForPos) != wildcardOne) break;
          if (searchInPos == searchInEnd) {
            return -1;
          }

          ++searchInPos;
        }

        if (searchForPos == searchForEnd) {
          return 0;
        }

        if (searchInPos == searchInEnd)
          return -1;
        char cmp;
        if (((cmp = searchForWildcard.charAt(searchForPos)) == wildcardEscape) && (searchForPos + 1 != searchForEnd))
        {
          cmp = searchForWildcard.charAt(++searchForPos);
        }

        ++searchForPos;
        do
        {
          while ((searchInPos != searchInEnd) && (Character.toUpperCase(searchIn.charAt(searchInPos)) != Character.toUpperCase(cmp)))
          {
            ++searchInPos;
          }
          if (searchInPos++ == searchInEnd) {
            return -1;
          }

          int tmp = wildCompare(searchIn, searchForWildcard);

          if (tmp <= 0) {
            return tmp;
          }
        }

        while ((searchInPos != searchInEnd) && (searchForWildcard.charAt(0) != wildcardMany));

        return -1;
      }
    }

    return ((searchInPos != searchInEnd) ? 1 : 0);
  }


  public static int lastIndexOf(byte[] s, char c) {
    if (s == null) {
      return -1;
    }

    for (int i = s.length - 1; i >= 0; --i) {
      if (s[i] == c) {
        return i;
      }
    }

    return -1;
  }

  public static int indexOf(byte[] s, char c) {
    if (s == null) {
      return -1;
    }

    int length = s.length;

    for (int i = 0; i < length; ++i) {
      if (s[i] == c) {
        return i;
      }
    }

    return -1;
  }

  public static boolean isNullOrEmpty(String toTest) {
    return ((toTest == null) || (toTest.length() == 0));
  }


  public static String sanitizeProcOrFuncName(String src)
  {
    if ((src == null) || (src.equals("%"))) {
      return null;
    }

    return src;
  }

  public static List<String> splitDBdotName(String src, String cat, String quotId, boolean isNoBslashEscSet)
  {
    if ((src == null) || (src.equals("%"))) {
      return new ArrayList();
    }

    boolean isQuoted = indexOfIgnoreCase(0, src, quotId) > -1;

    String retval = src;
    String tmpCat = cat;

    int trueDotIndex = -1;
    if (!(" ".equals(quotId)))
    {
      if (isQuoted) {
        trueDotIndex = indexOfIgnoreCase(0, retval, quotId + "." + quotId);
      }
      else
      {
        trueDotIndex = indexOfIgnoreCase(0, retval, ".");
      }
    }
    else {
      trueDotIndex = retval.indexOf(".");
    }

    List retTokens = new ArrayList(2);

    if (trueDotIndex != -1)
    {
      if (isQuoted) {
        tmpCat = toString(stripEnclosure(retval.substring(0, trueDotIndex + 1).getBytes(), quotId, quotId));

        if (startsWithIgnoreCaseAndWs(tmpCat, quotId)) {
          tmpCat = tmpCat.substring(1, tmpCat.length() - 1);
        }

        retval = retval.substring(trueDotIndex + 2);
        retval = toString(stripEnclosure(retval.getBytes(), quotId, quotId));
      }
      else
      {
        tmpCat = retval.substring(0, trueDotIndex);
        retval = retval.substring(trueDotIndex + 1);
      }
    }
    else {
      retval = toString(stripEnclosure(retval.getBytes(), quotId, quotId));
    }

    retTokens.add(tmpCat);
    retTokens.add(retval);
    return retTokens;
  }

  public static final boolean isEmptyOrWhitespaceOnly(String str) {
    if ((str == null) || (str.length() == 0)) {
      return true;
    }

    int length = str.length();

    for (int i = 0; i < length; ++i) {
      if (!(Character.isWhitespace(str.charAt(i)))) {
        return false;
      }
    }

    return true;
  }

  public static String escapeQuote(String src, String quotChar) {
    if (src == null) {
      return null;
    }

    src = toString(stripEnclosure(src.getBytes(), quotChar, quotChar));

    int lastNdx = src.indexOf(quotChar);

    String tmpSrc = src.substring(0, lastNdx);
    tmpSrc = tmpSrc + quotChar + quotChar;

    String tmpRest = src.substring(lastNdx + 1, src.length());

    lastNdx = tmpRest.indexOf(quotChar);
    while (lastNdx > -1)
    {
      tmpSrc = tmpSrc + tmpRest.substring(0, lastNdx);
      tmpSrc = tmpSrc + quotChar + quotChar;
      tmpRest = tmpRest.substring(lastNdx + 1, tmpRest.length());

      lastNdx = tmpRest.indexOf(quotChar);
    }

    tmpSrc = tmpSrc + tmpRest;
    src = tmpSrc;

    return src;
  }

  public static String toString(byte[] value, int offset, int length, String encoding)
    throws UnsupportedEncodingException
  {
    Charset cs = findCharset(encoding);

    return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
  }

  public static String toString(byte[] value, String encoding) throws UnsupportedEncodingException
  {
    Charset cs = findCharset(encoding);

    return cs.decode(ByteBuffer.wrap(value)).toString();
  }

  public static String toString(byte[] value, int offset, int length) {
    try {
      Charset cs = findCharset(platformEncoding);

      return cs.decode(ByteBuffer.wrap(value, offset, length)).toString();
    }
    catch (UnsupportedEncodingException e)
    {
    }
    return null;
  }

  public static String toString(byte[] value) {
    try {
      Charset cs = findCharset(platformEncoding);

      return cs.decode(ByteBuffer.wrap(value)).toString();
    }
    catch (UnsupportedEncodingException e)
    {
    }
    return null;
  }

  public static byte[] getBytes(String value, String encoding) throws UnsupportedEncodingException
  {
    Charset cs = findCharset(encoding);

    ByteBuffer buf = cs.encode(value);

    int encodedLen = buf.limit();
    byte[] asBytes = new byte[encodedLen];
    buf.get(asBytes, 0, encodedLen);

    return asBytes;
  }

  public static byte[] getBytes(String value) {
    try {
      Charset cs = findCharset(platformEncoding);

      ByteBuffer buf = cs.encode(value);

      int encodedLen = buf.limit();
      byte[] asBytes = new byte[encodedLen];
      buf.get(asBytes, 0, encodedLen);

      return asBytes;
    }
    catch (UnsupportedEncodingException e)
    {
    }
    return null;
  }

  public static final boolean isValidIdChar(char c) {
    return ("abcdefghijklmnopqrstuvwxyzABCDEFGHIGKLMNOPQRSTUVWXYZ0123456789$_#@".indexOf(c) != -1);
  }

  static
  {
    for (int i = -128; i <= 127; ++i) {
      allBytes[(i - -128)] = (byte)i;
    }

    String allBytesString = new String(allBytes, 0, 255);

    int allBytesStringLen = allBytesString.length();

    int i = 0;
    for (; (i < 255) && (i < allBytesStringLen); ++i) {
      byteToChars[i] = allBytesString.charAt(i);
    }
    try
    {
      toPlainStringMethod = BigDecimal.class.getMethod("toPlainString", new Class[0]);
    }
    catch (NoSuchMethodException nsme)
    {
    }
  }
}
