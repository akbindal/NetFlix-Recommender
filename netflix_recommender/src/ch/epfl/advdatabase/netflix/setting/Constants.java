package ch.epfl.advdatabase.netflix.setting;

public class Constants {
	public final static int D = 10;
	public static int NO_USER = 5000; //480189;
	public static int NO_MOVIES = 99;//17770;
	public static int U_FILES = 88;
	public static int V_FILES = 88;
	public static int U_SPLIT_SIZE = (int) Math.round(((double)NO_USER)/U_FILES+0.5); // =floor(U/150) where 150 is number of reducer
	public static int V_SPLIT_SIZE = (int) Math.round(((double)NO_MOVIES)/V_FILES+0.5);;
	
}
