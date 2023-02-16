SELECT airport_name 
     , airport_code
  FROM airline_statistics
 LIMIT 100; 

-- First question
SELECT airport_name
	 , airport_code
	 , sum(stat_flights_cancelled) total_cancelled_flights
	 , rank() OVER ( ORDER BY total_cancelled_flights DESC) AS rank_cancelled_flights
  FROM airline_statistics
 GROUP BY airport_name
	   ,  airport_code
 ORDER BY total_cancelled_flights DESC 
 LIMIT 3;

-- Second question
SELECT airport_name
	 , airport_code
	 , avg(stat_min_delayed_total) AS avg_delay
	 , rank() OVER (ORDER BY avg_delay DESC) AS rank_avg_delays
  FROM airline_statistics
 GROUP BY airport_name
	 ,    airport_code
 ORDER BY rank_avg_delays limit 3;

-- Third question 
SELECT airport_name
	 , airport_code
	 , avg(stat_min_delayed_total) AS avg_delay
	 , avg(stat_delays_late_aircraft) AS avg_late_aircraft
	 , rank() OVER (
		ORDER BY avg_late_aircraft DESC
		) AS rank_avg_delays_la
FROM airline_statistics
WHERE stat_delays_late_aircraft >= 3500
GROUP BY airport_name
	,airport_code
ORDER BY rank_avg_delays_la limit 3;

SELECT airport_name
	 , airport_code
	 , avg(stat_min_delayed_total) AS avg_delay
	 , max(stat_delays_weather) AS avg_delays_weather
	 , rank() OVER (ORDER BY avg_delays_weather DESC) AS rank_avg_delays_weather
  FROM airline_statistics
 GROUP BY airport_name
   	 ,    airport_code
 ORDER BY rank_avg_delays_weather limit 5;

SELECT airport_name
	 , airport_code
	 , avg(stat_min_delayed_total) AS avg_delay
	 , avg(stat_delays_security) AS avg_delays_security
	 , rank() OVER (ORDER BY avg_delays_security DESC) AS rank_avg_delays_security
  FROM airline_statistics
 GROUP BY airport_name
 	 ,    airport_code
	 ,    stat_delays_security
 ORDER BY rank_avg_delays_security limit 5;



     
   