package org.folio.services.mappers.processor;

import org.apache.commons.lang3.StringUtils;

/**
 * Escape text so that it is valid json as well as valid postgres jsonb data
 *
 */
public class Escaper {

  private static String removeEscapedChars(String text) {
    int len = text.length();
    StringBuilder token = new StringBuilder();
    boolean slash = false;
    boolean isEven = false;

    for (int j = 0; j < len; j++) {
      char t = text.charAt(j);
      //this is our record delimiter '|', so for now as a quick fix,
      //replace it with a blank
      if( t == '|'){
        t = ' ';
      }
      if (slash && isEven && t == '\\') {
        // we've seen \\ and now a third \ in a row
        isEven = false;
        slash = true;
        token.append(t);
      } else if (slash && !isEven && t == '\\') {
        // we have seen an odd number of \ and now we see another one, meaning \\ in the string
        slash = true;
        isEven = true;
        token.append(t);
      } else if (slash && !isEven && t != '\\') {
        // we've hit a non \ after a single \, this needs to get encoded to be \\
        token.append('\\').append(t);
        isEven = false;
        slash = false;
      } else if (!slash && t == '\\') {
        // we've hit a \
        token.append(t);
        isEven = false;
        slash = true;
      } else {
        // even number of slashes following by a non slash, or just a non slash
        token.append(t);
        isEven = false;
        slash = false;
      }
    }
    return token.toString();
  }


  /**
   * This function escapes data with two purposes in mind. The Marc data does not need to
   * conform to json or postgres escaped characters - this function takes Marc data and
   * escapes it so that it is valid in both a json and a postgres context
   * @param data
   * @return
   */
  public static String escape(String data){
    // remove \ char if it is the last char of the text
    if (data.endsWith("\\")) {
      data = data.substring(0, data.length() - 1);
    }
    data = removeEscapedChars(data).replaceAll("\\\"", "\\\\\"");
    return data;
  }

  /**
   * Escapes characters within a given json string to be able to 'COPY' to postgres jsonb
   * @param s json string to be escaped
   * @return escaped string
   */
  public static String escapeSqlCopyFrom(String s) {
    return StringUtils.replaceEach(s,
      new String[]{"\\", "|", "\n", "\r"},
      new String[]{"\\\\", "\\|", "\\\n", "\\\r"}
    );
  }

}
