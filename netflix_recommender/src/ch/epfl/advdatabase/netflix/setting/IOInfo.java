/**
 * 
 */
/**
 * @author ashish
 *
 */
package ch.epfl.advdatabase.netflix.setting;

public final class IOInfo {
	public final static String TEMP_MOVIE_OUTPUT="/std57/temp/movie/output";
	public final static String TEMP_MOVIE_MERGE="/std57/temp/movie/merge";
	public final static String TEMP_USER_OUTPUT="/std57/temp/user/output";
	public final static String TEMP_USER_MERGE="/std57/temp/movie/merge";
	public final static String MATRIX_OUTPUT="/std57/temp/matrix/outptu";
	public final static String MATRIX_MERGE="/std57/temp/matrix/merge";
	public final static String CACHE_ROW_MATRIX="/std57/cache/matrix/row";
	public final static String CACHE_COL_MATRIX="/std57/cache/matrix/col";
	public final static String getIterPath(int i, char c) {
		if(c=='u') {
			return "/std57/temp/U_"+i;
		} else if (c=='v') {
			return "/std57/temp/V_"+i;
		}
		return null;
	}
	
}