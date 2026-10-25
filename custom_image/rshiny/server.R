server <- function(input, output){
	selected_data <- reactive({
        	# Connect to the DB
        	conn <- dbConnect(
                  RPostgres::Postgres(),
                  dbname = Sys.getenv("POSTGRES_DB"),
                  host = "postgres_custom",
                  port = "5432",
                  user = Sys.getenv("POSTGRES_USER"),
                  password = Sys.getenv("POSTGRES_PASSWORD")
		)
        	on.exit(dbDisconnect(conn))
        	# Get the data
        	corona <- dbGetQuery(conn, glue("SELECT 
						 to_date(substring(\"S_DT\",1,10),'YYYY.MM.DD') as s_dt
					       	,\"N_HJ\"::float as n_hj
						,\"T_HJ\" as t_hj
						FROM tbcorona19countstatus_bulk2 
						WHERE to_date(substring(\"S_DT\",1,10),'YYYY.MM.DD') BETWEEN '{format(input$dates[1])}' AND '{format(input$dates[2])}'"))
        	# Convert to data.frame
        	data.frame(corona)
	})
	
	output$daily_confirmed <- renderPlot({
		ggplot(data=selected_data(), aes(x=s_dt, y=n_hj)) + 
			geom_line(color='blue', linewidth = 1) + 
			geom_point(color='red') + 
			geom_smooth(method='lm') +
			ggtitle("Daily confirmed cases") +
			labs(x='Date',y='Daily confirmed cases')
	})
	output$total_confirmed <- renderPlot({
               ggplot(data=selected_data(), aes(x=s_dt, y=t_hj)) +
               geom_line(color='blue', linewidth = 1) +
               geom_point(color='red') +
               geom_smooth(method='lm') +
	       ggtitle("Total confirmed cases") +
               labs(x='Date',y='Total confirmed cases')
        })

}