/**
 * 
 */
/**
 * @author ashish
 *
 */
package ch.epfl.advadb.setting;

public final class IOInfo {
	public final static String TEMP_MOVIE_OUTPUT="/std57/temp/movie/output";
	public final static String TEMP_MOVIE_MERGE="/std57/temp/movie/merge";
	public final static String TEMP_USER_OUTPUT="/std57/temp/user/output";
	public final static String TEMP_USER_MERGE="/std57/temp/user/merge";
	public final static String MATRIX_OUTPUT="/std57/temp/matrix/outptu";
	public final static String MATRIX_MERGE="/std57/temp/matrix/merge";
	public final static String CACHE_ROW_MATRIX="/std57/cache/matrix/row";
	public final static String CACHE_COL_MATRIX="/std57/cache/matrix/col";
	public static final String CACHE_PRE_UV = "/std57/cache/preuv";
	public static  String OUTPUT_U_INITIALIZATION;
	public static String OUTPUT_V_INITIALIZATION;
	public static String TRASH = "/std57/trash/dump.txt";
	public static String TEMP_JOIN_UM = "/std57/temp/join/um";
	public static String OUTPUT_V;
	public static String OUTPUT_U;
	
	public final static String getIterPath(int i, char c) {
		if(c=='u') {
			return "/std57/temp/U_"+i;
		} else if (c=='v') {
			return "/std57/temp/V_"+i;
		}
		return null;
	}
	
}