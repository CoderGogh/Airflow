FROM rocker/shiny-verse:4.6.1
WORKDIR /app
COPY . /app
EXPOSE 3838
CMD R -e 'shiny::runApp("global.R", port=3838, host="0.0.0.0")'
