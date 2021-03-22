Histogram

01.	Program Details:

	(i)	Date: 25th February, 2020 and 3rd March, 2020 respectively
	(ii)	Language: Java (MapReduce)

02.	Problem Statement:

	(i)	Input: RGB color codes (might be repeated).
		Example: 12 123 243 - Where 12 is intensity of red, 123 is intensity of green, and 243 is intensity of blue.

	(ii)	Output: The colors with their corresponding intensity and the total number of times for which the color intensity is repeated.
		Example: 2 123 108548 - Here, 2 is green, 123 is intensity corresponding to the clor green, and
		the color green with intensity 123 is repeated 108540 times.

03.	Working:

	I have included 2 of 3 different ways this program in Hadoop MapReduce can work.
	(i)	In first program (Histogram01.java), everything is handled by 1 job.

	(ii)	In second program (Histogram02.java),
		2 jobs are used and a Combiner is used along with the Mappers and Reducers.

04.	Code Execution:

	To run this program, you need to setup Hadoop system,
	or you can run it on server.

05.	Note: I have included sample input and output .txt files.
