/**
 * 
 */
/**
 * @author ashish
 *
 */
package ch.epfl.advadb.setting;

public final class IOInfo {
	
	public final static String TEMP_MOVIE_MERGE="/std57/temp/movie/merge";
	
	public final static String TEMP_USER_MERGE="/std57/temp/user/merge";
	public final static String MATRIX_MERGE="/std57/temp/matrix/merge";
	public static String CACHE_ROW_MATRIX;
	public static String CACHE_COL_MATRIX;
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