


setwd("/Users/mpigman/Library/CloudStorage/GoogleDrive-mpigman@lbl.gov/Shared drives/Buildings Policy Analysis Team/Projects/Buildings Standard Scenarios/v1/Annual_Results/bss-batch_012725")

library(tidyverse)
library(scales)
library(cowplot)
theme_set(theme_bw())


# BSS results created by SQL queries --------------------------------------

# the data frame names are analogous to the names of the SQL queries that created them

input_dir <- "generated_csvs" #directory where the csvs are stored
filename_prefix <- ""
graph_dir <- "graphs" #directory where the graphs will be written

county_ann_eu<-read_csv(paste0(input_dir,"/",filename_prefix,"county_ann_eu.csv")) %>% mutate(turnover=factor(turnover,levels=c("baseline","stated","mid","high","breakthrough","ineff"),ordered=T))
county_100_hrs<-read_csv(paste0(input_dir,"/",filename_prefix,"county_100_hrs.csv")) %>% mutate(turnover=factor(turnover,levels=c("baseline","stated","mid","high","breakthrough","ineff"),ordered=T))
county_monthly_maxes<-read_csv(paste0(input_dir,"/",filename_prefix,"county_monthly_maxes.csv")) %>% mutate(turnover=factor(turnover,levels=c("baseline","stated","mid","high","breakthrough","ineff"),ordered=T))

county_hourly_examples_stated<-read_csv(paste0(input_dir,"/",filename_prefix,"county_hourly_examples_stated.csv"))
county_hourly_examples_mid<-read_csv(paste0(input_dir,"/",filename_prefix,"county_hourly_examples_mid.csv"))
county_hourly_examples_high<-read_csv(paste0(input_dir,"/",filename_prefix,"county_hourly_examples_high.csv"))
county_hourly_examples_breakthrough<-read_csv(paste0(input_dir,"/",filename_prefix,"county_hourly_examples_breakthrough.csv"))
county_hourly_examples_ineff<-read_csv(paste0(input_dir,"/",filename_prefix,"county_hourly_examples_ineff.csv"))

county_hourly_examples_list <- list(
  county_hourly_examples_stated,
  county_hourly_examples_mid,
  county_hourly_examples_high,
  county_hourly_examples_breakthrough,
  county_hourly_examples_ineff
)

state_monthly_2024<-read_csv(paste0(input_dir,"/",filename_prefix,"state_monthly_2024.csv"))

state_ann<-read_csv(paste0(input_dir,"/state_ann_eu_stage_fossil.csv")) %>% mutate(turnover=factor(turnover,levels=c("baseline","stated","mid","high","breakthrough","ineff"),ordered=T)) %>%
    # this won't be needed once the measure map from 1/29/2025 is used
    mutate(description=case_when(description=="HP boiler, best" ~ "Boiler, best HP",
                               description=="Water heater, HP, ESTAR 240V" ~ "Water heater, HP, ESTAR",
                               description=="Water heater, HP, ESTAR 120V" ~ "Water heater, HP, ESTAR",
                               TRUE ~ description))


# map data ----------------------------------------------------------------

library(maps)
library(mapdata)
library(colorspace)

#shape files for counties 
county_map<-map_data("county") %>% filter(region!="hawaii")

# for mapping of county designations, population
geo_counties<-read_csv("../map_meas/emm_county_map.csv") %>% filter(subregion!="yellowstone national") #we don't need 2 yellowstones


# for labeling ---------------------------------------------------------------

# Scout scenarios -- every value of "turnover" should be here
to<-c(baseline="Reference",breakthrough="Breakthrough",ineff="Inefficient",high="High",mid="Mid",stated_policies="Stated Policies",stated="Stated Policies",
      accel="Accelerated Innovation",fossil="High Fuel Demand",ref="BAU",aeo="AEO",state="State and Local Action",brk="Breakthrough")
# sector
s_label<-c(com="Commercial",res="Residential",all="All Buildings")
# end uses
eu<-c(`Computers and Electronics`="Computers and Electronics",Cooking="Cooking",`Cooling (Equip.)`="Cooling",`Heating (Equip.)`="Heating",Lighting="Lighting",Other="Other",Refrigeration="Refrigeration",Ventilation="Ventilation",`Water Heating`="Water Heating")


#fill colors for end use annual graphs
colors<-c("#e41a1c","#fbb4ae","#377eb8","#b3cde3","#4daf4a","#ccebc5","#984ea3","#decbe4","#ff7f00","#fed9a6","#ffee33","#ffffcc","#a65628","#e5d8bd","#f781bf","#fddaec","#999999","#f2f2f2")
#for % change maps
#original
color_interp <- gradient_n_pal(colours = c("#80cdc1", "#f5f5f5", "#dfc27d", "#bf812d", "#8c510a","#583A17", "black"), 
                               values = c(-1, 0, 1, 2, 5,20,90), space = "Lab")
#a bit darker
color_interp <- gradient_n_pal(colours = c("#35978f", "#f5f5f5",  "#bf812d", "#8c510a","#583A17", "black"), 
                               values = c(-1, 0, 1, 2, 5,90), space = "Lab")

#more extreme
color_interp <- gradient_n_pal(colours = c("#2A7062","#80cdc1", "#f5f5f5", "#dfc27d", "#bf812d", "#8c510a","#583A17", "black"), 
                               values = c(-1, -.25, 0, .25, .5, 1,3,20), space = "Lab")
#for top 100 hrs maps
diverg_interp<-gradient_n_pal(colours=c("#FDAB31","#E5E8C1","#3F7DDE"),
                              values=c(0,0.5,1),space="Lab")
#for winter/summer ratio
diverg_ratio <- gradient_n_pal(colours = c("#FA9C26","#F7AF50","#E5E8C1","#7186CE","#3166D7"), 
                               values = c(-1.1,-.5,0,.5,1.1), space = "Lab")

#function to round a number to the closest __
round_any<-function(x, accuracy, f=round){f(x/ accuracy) * accuracy}


#sample sizes and county names
ns<-arrow::read_parquet("ResStock2024_2baseline_amy.parquet",col_select = c("bldg_id","in.county","in.county_name","in.state")) %>% group_by(in.county,in.county_name,in.state) %>% summarize(n=n()) %>%
  mutate(county_name=paste0(in.state,", ", in.county_name)) %>% ungroup() %>%
  select(in.county,n,county_name)


#width for annual graphs changeable based on number of scenarios
width<-(1+length(unique(state_ann$turnover)))*1.8


# map with histogram - functions -----------------------------------------------------------------



# Function to create main and inset plots
create_plot_pair <- function(data) {
  list(
    main = county_map %>% ggplot()+
      geom_polygon(data = data%>%
                     full_join(geo_counties,by=c("in.county"="stock.county"),relationship="many-to-many") %>%
                     full_join(county_map,by=c("region","subregion"),relationship="many-to-many") %>%
                     filter(!is.na(turnover)),
                   mapping = aes(x = long, y = lat, group = group,fill=fill_color),color=NA) +
      coord_map("conic", lat0 = 30,xlim = c(-66.95,-124.67),ylim=c(49.38,25.84))+
      scale_fill_identity() +
      theme(panel.grid = element_blank(),axis.title = element_blank(),axis.line = element_blank(),axis.text = element_blank(),axis.ticks = element_blank(),panel.border = element_rect(color=NA),
            axis.ticks.length = unit(0, "pt"), #length of tick marks
            legend.position = "none",
            panel.background = element_rect(fill="white",color=NA),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            plot.background = element_rect(fill="white",color=NA),
            plot.margin = unit(c(0,0,0,0),"mm")),
    inset = data %>%
      mutate(percent_binned=if_else(percent_change>3,3,round_any(percent_change,.1)),
             fill_color=color_interp(percent_binned))%>%
      ggplot(aes(x=percent_binned,fill=fill_color))+
      geom_bar(stat="count",color="gray20")+
      scale_fill_identity() +
      scale_x_continuous(labels=percent_format(),limits=c(-1,3.05),expand=expansion(add=0,mult=c(0,.05)))+
      scale_y_continuous(n.breaks=3,expand=expansion(add=0,mult=c(0,.05)))+
      theme(panel.grid.minor.y = element_blank(),axis.title = element_blank(),plot.margin=margin(b = 0))
  )
}

# function to combine and format the map and histogram plots
plot_map_hist<-function(data_list){
  # Generate plot pairs for each subset
  plot_pairs <- purrr::imap(datasets, ~ create_plot_pair(
    .x
  ))
  
  combined_plots <- purrr::map(plot_pairs, ~ plot_grid(.x$main, plot_grid(NULL,.x$inset,NULL,rel_widths=c(1,4,1),ncol=3), rel_heights = c(4, 1), rel_widths = c(5,4),ncol=1))
  
  nice_labels <- setNames(
    purrr::map_chr(names(combined_plots), ~ {
      parts <- str_split(.x, "\\.", n = 2)[[1]] # Ensure only 2 parts: turnover and end_use/sector
      
      # Extract turnover and end_use parts
      turnover_label <- to[parts[1]] # First part is turnover
      subset_label <- c(eu,s)[parts[2]] # Second part is end_use or sector
      
      # Combine into a single label, skipping NA
      paste(na.omit(c(turnover_label, subset_label)), collapse = " - ")
    }),
    names(combined_plots) # Assign names
  )

  # Add titles to each plot using the nice labels
  annotated_plots <- purrr::imap(combined_plots, ~ {
    label <- nice_labels[.y] 
    ggdraw() +
      draw_plot(.x) +
      draw_text(label, x = .5, y = .25, vjust = 1.5, hjust = 0.5, size = 12)
  })
  
  nrows<-if_else(sum(str_detect(names(datasets),pattern = "."))>=1,2,1)
  if(length(nice_labels)==0){return(plot_grid(plotlist = combined_plots, nrow = nrows))}
  else{
    return(plot_grid(plotlist = annotated_plots, nrow = nrows))}
  
}



# map with histogram - plot % changes! ----------------------------------------------

# by turnover
aggregated<-county_ann_eu %>%
  group_by(turnover,in.state,in.county,year) %>% summarize(county_ann_kwh=sum(county_ann_kwh))
filename<-"county_map_ann_2050vs2024_all"
plottitle<-"Change in Building Electricity: 2024 to 2050"
annual_county_change<- aggregated %>%
  filter(year %in% c(2024,2050)) %>%
  pivot_wider(names_from=year,values_from=county_ann_kwh) %>%
  mutate(percent_change=`2050`/`2024`-1,
         fill_color=color_interp(percent_change))
datasets <- split(annual_county_change, list(annual_county_change$turnover))

p<-plot_map_hist(datasets)
save_plot(paste0(graph_dir,"/",filename_prefix,filename,".jpg"),
          plot_grid(ggdraw()+draw_text(plottitle,x=.5,y=.5,vjust=.5,hjust=.5),p,nrow = 2,rel_heights = c(.05,1)),
          base_height = 12,bg="white")


# by turnover, 50+ RS samples
aggregated<-county_ann_eu %>%
  right_join(ns %>% filter(n>=50),by="in.county") %>%
  group_by(turnover,in.state,in.county,year) %>% summarize(county_ann_kwh=sum(county_ann_kwh))
filename<-"county_map_ann_2050vs2024_all_50plus"
plottitle<-"Change in Building Electricity: 2024 to 2050\nCounties with 50+ ResStock Samples"
annual_county_change<- aggregated %>%
  filter(year %in% c(2024,2050)) %>%
  pivot_wider(names_from=year,values_from=county_ann_kwh) %>%
  mutate(percent_change=`2050`/`2024`-1,
         fill_color=color_interp(percent_change))
datasets <- split(annual_county_change, list(annual_county_change$turnover))

p<-plot_map_hist(datasets)
save_plot(paste0(graph_dir,"/",filename_prefix,filename,".jpg"),
          plot_grid(ggdraw()+draw_text(plottitle,x=.5,y=.5,vjust=.5,hjust=.5),p,nrow = 2,rel_heights = c(.05,1)),
          base_height = 12,bg="white")


# by turnover and sector
aggregated<-county_ann_eu %>%
  group_by(turnover,in.state,in.county,year,sector) %>% summarize(county_ann_kwh=sum(county_ann_kwh))
filename<-"county_map_ann_2050vs2024_sector"
plottitle<-"Change in Building Electricity: 2024 to 2050"
annual_county_change<- aggregated %>%
  filter(year %in% c(2024,2050)) %>%
  pivot_wider(names_from=year,values_from=county_ann_kwh) %>%
  mutate(percent_change=`2050`/`2024`-1,
         fill_color=color_interp(percent_change))
datasets <- split(annual_county_change, list(annual_county_change$turnover,annual_county_change$sector))

p<-plot_map_hist(datasets)
save_plot(paste0(graph_dir,"/",filename_prefix,filename,".jpg"),
          plot_grid(ggdraw()+draw_text(plottitle,x=.5,y=.5,vjust=.5,hjust=.5),p,nrow = 2,rel_heights = c(.05,1)),
          base_width = 20,bg="white",base_height = 8)


# res HVAC by turnover
aggregated<-county_ann_eu %>%
  filter(sector=="res",end_use %in% c("Heating (Equip.)","Cooling (Equip.)"))
filename<-"county_map_ann_2050vs2024_res_hvac"
plottitle<-"Change in Residential HVAC Electricity: 2024 to 2050"
annual_county_change<- aggregated %>%
  filter(year %in% c(2024,2050)) %>%
  pivot_wider(names_from=year,values_from=county_ann_kwh) %>%
  mutate(percent_change=`2050`/`2024`-1,
         fill_color=color_interp(percent_change))
datasets <- split(annual_county_change, list(annual_county_change$turnover,annual_county_change$end_use))

p<-plot_map_hist(datasets)
save_plot(paste0(graph_dir,"/",filename_prefix,filename,".jpg"),
          plot_grid(ggdraw()+draw_text(plottitle,x=.5,y=.5,vjust=.5,hjust=.5),p,nrow = 2,rel_heights = c(.05,1)),
          base_width = 20,bg="white",base_height = 8)

# com HVAC by turnover
aggregated<-county_ann_eu %>%
  filter(sector=="com",end_use %in% c("Heating (Equip.)","Cooling (Equip.)"))
filename<-"county_map_ann_2050vs2024_com_hvac"
plottitle<-"Change in Commercial Heating and Cooling Electricity: 2024 to 2050"
annual_county_change<- aggregated %>%
  filter(year %in% c(2024,2050)) %>%
  pivot_wider(names_from=year,values_from=county_ann_kwh) %>%
  mutate(percent_change=`2050`/`2024`-1,
         fill_color=color_interp(percent_change))
datasets <- split(annual_county_change, list(annual_county_change$turnover,annual_county_change$end_use))

p<-plot_map_hist(datasets)
save_plot(paste0(graph_dir,"/",filename_prefix,filename,".jpg"),
          plot_grid(ggdraw()+draw_text(plottitle,x=.5,y=.5,vjust=.5,hjust=.5),p,nrow = 2,rel_heights = c(.05,1)),
          base_width = 20,bg="white",base_height = 8)



# top 100 hours - map and histogram of share in the winter -----------------------------------------------------------

#county_100_hrs<-county_100_hrs %>% filter(turnover %in% c("baseline","breakthrough","high,"ineff"),year %in% c(2024,2030,2040,2050)) %>% droplevels()
# filename_prefix<-"ref_high_brk_ineff_"

county_100_hrs_share<-county_100_hrs %>%
  mutate(month=month(timestamp_hour),season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(turnover,year,in.county) %>%
  mutate(share_winter=sum(season=="Winter")/100) %>%
  filter(rank_num==1)

top100_map<- county_100_hrs_share %>% 
  left_join(geo_counties %>% select(stock.county,subregion,region,population),by=c("in.county"="stock.county"),relationship="many-to-many") %>%
  full_join(county_map,by=c("region","subregion"),relationship="many-to-many") %>%
  filter(!is.na(turnover),!is.na(year))


p100<-county_map %>% ggplot()+
  geom_polygon(data = top100_map , 
               mapping = aes(x = long, y = lat, group = group,fill=share_winter),color=NA) +
  coord_map("conic", lat0 = 30)+
  scale_fill_gradient2(low="#FDAB31",mid="#E5E8C1",midpoint=.5,high="#3F7DDE",name="",labels=percent_format())+
  theme(panel.grid = element_blank(),axis.title = element_blank(),axis.line = element_blank(),axis.text = element_blank(),axis.ticks = element_blank(),panel.border = element_blank(),
        panel.background = element_blank(),
        legend.position="bottom",legend.key.width = unit(1,"in"),
        panel.spacing = unit(0, "in"),
        strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))+
  facet_grid(year~turnover,labeller = labeller(turnover=to))+
  ggtitle("Share of Top 100 Hours in the Winter")
save_plot(paste0(graph_dir,"/",filename_prefix,"county_100_hrs_share.jpg"),p100,base_height = 6,bg="white")

p100_hist<-county_100_hrs_share %>%
  mutate(percent_binned=round_any(share_winter,.05),
         fill_color=diverg_interp(percent_binned))%>%
  ggplot(aes(x=percent_binned,y=after_stat(count/3107),fill=fill_color))+
  geom_bar()+
  scale_fill_identity() +
  facet_grid(year~turnover,labeller = labeller(turnover=to))+
  scale_x_continuous(labels=percent_format())+
  scale_y_continuous(n.breaks=4,expand=expansion(add=0,mult=c(0,.05)),labels=percent_format())+
  theme(panel.grid.minor.y = element_blank(),axis.title = element_blank(),plot.margin=margin(b = 0),
        strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))+
  ggtitle("Share of Top 100 Hours in the Winter")
save_plot(paste0(graph_dir,"/",filename_prefix,"county_100_hrs_share_hist.jpg"),p100_hist,base_height = 4,base_width = 7,bg="white")


# winter to summer ratio - map and histogram --------------------------------------------------


peak_ratio<-county_monthly_maxes %>%
  mutate(month=month(timestamp_hour),season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(in.county,turnover,year,season) %>% filter(county_total_hourly_kwh==max(county_total_hourly_kwh)) %>%
  select(in.county,turnover,in.state,year,season,county_total_hourly_kwh) %>%
  pivot_wider(names_from=season,values_from=county_total_hourly_kwh) %>%
  mutate(winter_to_summer=Winter/Summer,log_winter_to_summer=log10(winter_to_summer))

peak_ratio_map<- peak_ratio %>% 
  left_join(geo_counties %>% select(stock.county,subregion,region,population),by=c("in.county"="stock.county"),relationship="many-to-many") %>%
  full_join(county_map,by=c("region","subregion"),relationship="many-to-many") %>%
  filter(!is.na(turnover),!is.na(year))


ratio_map<-county_map %>% ggplot()+
  geom_polygon(data = peak_ratio_map , 
               mapping = aes(x = long, y = lat, group = group,fill=log_winter_to_summer),color=NA) +
  coord_map("conic", lat0 = 30)+
  scale_fill_gradient2(low="#FA9C26",mid="#E5E8C1",midpoint=0,high="#3166D7",name="",labels = function(x) round(10^x, 2),breaks=c(-1,-.6,-.3,0,.3,.6,1))+
  theme(panel.grid = element_blank(),axis.title = element_blank(),axis.line = element_blank(),axis.text = element_blank(),axis.ticks = element_blank(),panel.border = element_blank(),
        panel.background = element_blank(),
        legend.position="bottom",legend.key.width = unit(1,"in"),
        panel.spacing = unit(0, "in"),
        strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))+
  facet_grid(year~turnover,labeller = labeller(turnover=to))+
  ggtitle("Ratio of Winter Peak to Summer Peak")
save_plot(paste0(graph_dir,"/",filename_prefix,"county_ratio.jpg"),ratio_map,base_height = 12,bg="white")


ratio_hist<-peak_ratio %>% filter(turnover %in% c("baseline","high","breakthrough","ineff"),year %in% c(2024,2030,2040,2050)) %>% droplevels() %>%
  mutate(log_ratio_binned=round_any(log_winter_to_summer,.02),
         fill_color=diverg_ratio(log_ratio_binned))%>%
  ggplot(aes(x=log_ratio_binned,y=after_stat(count/3107),fill=fill_color))+
  geom_vline(xintercept=0,color="gray50")+
  geom_bar(stat="count")+
  scale_fill_identity() +
  facet_grid(year~turnover,labeller = labeller(turnover=to))+
  scale_x_continuous(labels= function(x) round(10^x, 2))+
  coord_cartesian(xlim=c(log10(.25),log10(4)))+
  scale_y_continuous(n.breaks=4,expand=expansion(add=0,mult=c(0,.05)),labels=percent_format())+
  theme(panel.grid.minor.y = element_blank(),axis.title = element_blank(),plot.margin=margin(b = 0),
        strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))+
  ggtitle("Ratio of Winter Peak to Summer Peak")
save_plot(paste0(graph_dir,"/",filename_prefix,"county_ratio_hist.jpg"),ratio_hist,base_height = 6,base_width = 12,bg="white")



# example counties - peak day line plots --------------------------------------------------------
#peak day is selected based on the scenario (not the baseline)


for (i in 1:length(county_hourly_examples_list)){
with_ex_labels<-county_hourly_examples_list[[i]]%>% 
  left_join(ns %>% select(in.county,county_name),by="in.county") %>%
  mutate(example_label=paste0(example_type,": ",county_name))
day_labels<-with_ex_labels %>%   filter(year %in% c(2024,2050),turnover_max!="baseline") %>%
  select(in.county,year,month,day,example_label,turnover) %>% unique() %>%
  mutate(day_label=paste(month,day,sep="/"),x=if_else(year==2024,3,13))
psample<-with_ex_labels %>%
  filter(year %in% c(2024,2050),turnover_max!="baseline") %>%
  ggplot(aes(color=factor(year)))+
  geom_line(aes(x=hour,y=county_total_hourly_kwh/1000,linetype=turnover))+
  xlab("EST")+
  geom_text(data=day_labels,y=-Inf,vjust=-.5,aes(x=x,label=day_label),show.legend = F,size=8) +
  scale_y_continuous(name="MWh",labels=comma_format(),limits=c(0,NA))+
  scale_color_brewer(name="",palette = "Dark2")+
  scale_linetype(name="",labels=to)+
  facet_wrap(~example_label,scales="free",ncol=2)+
  ggtitle("Day with the Peak Hour")
save_plot(paste0(graph_dir,"/example_peak_days_",day_labels[1,"turnover"],".jpg"),psample,base_height = 18,base_width = 8,bg="white")
}

# compare state-level seasonal ratios to EIA ------------------------------

#https://drive.google.com/open?id=1btgB7_rSUJSTANdQ_kgGN3evd1baOQf8&usp=drive_fs
eia<-read_csv("/Users/mpigman/Library/CloudStorage/GoogleDrive-mpigman@lbl.gov/Shared drives/Buildings Standard Scenarios/Workflow design/Comparison Data/eia_gas_and_electricity_by_state_sector_year_month.csv")

eia_ratios_sector<-eia %>% filter(fuel=="electricity",sector %in% c("residential","commercial")) %>% 
  mutate(month=(match(month, month.abb)),
         season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder"),
         in.state=if_else(state=="District of Columbia","DC",state.abb[match(state,state.name)]),
         sector=if_else(sector=="commercial","com","res")) %>%
  group_by(state,in.state,year,month,season,sector) %>% summarize(sales.kWh=sum(sales.kWh)) %>%
  group_by(state,in.state,year,season,sector) %>% summarize(sales.kWh_max=max(sales.kWh)) %>%
  pivot_wider(names_from=season,values_from=sales.kWh_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer)

bss_ratios_sector<-state_monthly_2024 %>%
  mutate(season=case_when(month %in% 5:9 ~ "Summer", month %in% c(11,12,1,2) ~ "Winter", TRUE ~ "Shoulder")) %>%
  group_by(in.state,sector,season) %>% summarize(monthly_max=max(state_monthly_kwh)) %>%
  pivot_wider(names_from=season,values_from=monthly_max) %>%
  mutate(max_winter_to_max_summer=Winter/Summer) %>%
  arrange(max_winter_to_max_summer)

eia_comp<-eia_ratios_sector %>%  filter(year<2024) %>%
  ggplot(aes(x=in.state,y=max_winter_to_max_summer))+
  geom_hline(yintercept = 1,color="grey30")+
  geom_boxplot()+
  geom_point(data=bss_ratios_sector ,color="red")+
  scale_x_discrete(limits=bss_ratios_sector[bss_ratios_sector$sector=="res",]$in.state,name="")+
  ylab("winter max / summer max")+
  facet_wrap(~sector,nrow=2,labeller = labeller(sector=s_label))+
  ggtitle("Ratio of max monthly electricity consumption in the winter max to max monthly electricity consumption in the summer",subtitle = "EIA 861 2001-2023 (boxplot) vs. BSS 2024 baseline (red dot)")
save_plot(paste0(graph_dir,"/",filename_prefix,"/eia_seasonal_ratio_comp.jpg"),eia_comp,base_height = 6,base_width = 12,bg="white")



# annual, national --------------------------------------------------------

# area plot by year, scenario, end use, sector

#electric all scenarios
filename_prefix<-""
state_ann %>%
  filter(fuel=="Electric") %>%
  group_by(year,sector,end_use,turnover) %>% 
  summarize(twh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=twh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=s_label))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors)+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario.jpeg"),device = "jpeg",width = width, height =4,units = "in")

#electric no stated policies
filename_prefix<-"no_stated_"
state_ann %>%
  filter(turnover!="stated",fuel=="Electric") %>%
  group_by(year,sector,end_use,turnover) %>% 
  summarize(twh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=twh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=s_label))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors)+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario.jpeg"),device = "jpeg",width = width, height =4,units = "in")


#fossil all scenarios
filename_prefix<-""
state_ann %>%
  filter(fuel!="Electric") %>%
  group_by(year,sector,end_use,turnover) %>% 
  summarize(twh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=twh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=s_label))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors[c(2,3,4,6,9)])+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario_fossil.jpeg"),device = "jpeg",width = width, height =4,units = "in")


#fossil no stated policies
filename_prefix<-"no_stated_"
state_ann %>%
  filter(turnover!="stated",fuel!="Electric") %>%
  group_by(year,sector,end_use,turnover) %>% 
  summarize(twh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=twh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=s_label))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors[c(2,3,4,6,9)])+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario_fossil.jpeg"),device = "jpeg",width = width, height =4,units = "in")


# annual, national by tech type ------------------------------------------------------------

agg <-state_ann %>% filter(fuel=="Electric") %>% group_by(year,end_use,turnover,sector,description) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>% ungroup() 


#all scenarios
filename_prefix<-""
#HVAC
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter((end_use %in% c("Cooling (Equip.)","Heating (Equip.)","Ventilation")),sector==sec,description!="fossil") %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(end_use~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_hvac.jpeg"),device = "jpeg",width = width, height =4,units = "in")
}


#water heating
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter((end_use %in% c("Water Heating")),sector==sec,description!="fossil") %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_wh.jpeg"),device = "jpeg",width = width, height =4/1.5,units = "in")
}


#non HVAC, non-WH
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0) %>% 
    filter(!(end_use %in% c("Water Heating","Heating (Equip.)","Cooling (Equip.)","Ventilation")),description!="fossil",sector==sec) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_non-mech.jpeg"),device = "jpeg",width = width, height =4/1.15,units = "in")
}



#no stated policies
filename_prefix<-"no_stated_"
#HVAC
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0, turnover!="stated") %>% 
    filter((end_use %in% c("Cooling (Equip.)","Heating (Equip.)","Ventilation")),description!="fossil",sector==sec) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(end_use~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_hvac.jpeg"),device = "jpeg",width = width, height =5,units = "in")
}


#water heating
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0, turnover!="stated") %>% 
    filter(end_use %in% c("Water Heating"),sector==sec,description!="fossil") %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_wh.jpeg"),device = "jpeg",width = width, height =4/1.5,units = "in")
}


#non HVAC, non-WH
for (sec in c("com","res")){
  agg%>%  group_by(description) %>% filter(sum(TWh)>0, turnover!="stated") %>% 
    filter(!(end_use %in% c("Water Heating","Heating (Equip.)","Cooling (Equip.)","Ventilation")),sector==sec) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",sec,"_non-mech.jpeg"),device = "jpeg",width = width, height =4/1.15,units = "in")
}


# annual by state and tech type for heating and cooling  ------------------------------------------------------------

# states to show as examples for heating and cooling
states<-c("WA","CA","MA","FL")

agg_state <-state_ann %>% filter(reg %in% states,fuel=="Electric") %>% 
  group_by(year,reg,end_use,turnover,sector,description) %>% 
  summarize(TWh=sum(state_ann_kwh)/10^9) %>%
  ungroup()

#all scenarios
filename_prefix<-"all_"
for(sec in c("res","com")){
  for (u in c("Heating (Equip.)","Cooling (Equip.)")){
    agg_state%>% 
      group_by(description) %>% filter(sum(TWh)>0) %>%
      filter(end_use ==u,sector==sec) %>%
      ggplot(aes(x=year,y=TWh,fill=description)) +
      geom_area()+
      facet_grid(reg~turnover,labeller = labeller(turnover=to,end_use=eu),scales="free")+
      scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)))+ 
      scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
      scale_fill_manual(values=colors,name="")+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
    ggsave(paste0(graph_dir,"/",filename_prefix,"state_annual_",sec,"_",eu[u],".jpeg"),device = "jpeg",width = width, height =width/8*length(states),units = "in",limitsize = FALSE)
  }
}

#no stated policies
filename_prefix<-"no_stated_sc_"
for(sec in c("res","com")){
  for (u in c("Heating (Equip.)","Cooling (Equip.)")){
    agg_state%>% 
      group_by(description) %>% filter(sum(TWh)>0,turnover!="stated") %>%
      filter(end_use ==u,sector==sec) %>%
      ggplot(aes(x=year,y=TWh,fill=description)) +
      geom_area()+
      facet_grid(reg~turnover,labeller = labeller(turnover=to,end_use=eu),scales="free")+
      scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)))+ 
      scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
      scale_fill_manual(values=colors[2:18],name="")+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=12),strip.text.x = element_text(size=12))
    ggsave(paste0(graph_dir,"/",filename_prefix,"state_annual_",sec,"_",eu[u],".jpeg"),device = "jpeg",width = width*.7, height =width*.85/2,units = "in")
  }
}


# 2050 end use comparison -------------------------------------------------

#all buildings
state_ann %>% filter(year==2050,fuel=="Electric") %>% group_by(turnover,end_use) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>% 
  ggplot(aes(x=turnover,y=TWh,fill=turnover))+
  geom_col(position="dodge")+
  scale_fill_brewer(name="",palette="Paired",labels=to)+scale_x_discrete(labels=eu,name="")+
  scale_y_continuous(expand=expansion(add=0,mult=c(0,.05)))+
  facet_grid(~end_use,labeller=labeller(end_use=eu))+
  theme(axis.text.x = element_blank(),axis.ticks.x=element_blank(),panel.grid.major.x = element_blank())
ggsave(paste0(graph_dir,"/national_annual_2050_by_eu.jpeg"),device = "jpeg",width = 10, height =2,units = "in")


#by sector
state_ann %>% filter(year==2050,fuel=="Electric") %>% group_by(turnover,end_use,sector) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>% 
  ggplot(aes(x=turnover,y=TWh,fill=turnover))+
  geom_col(position="dodge")+
  scale_fill_brewer(name="",palette="Paired",labels=to)+scale_x_discrete(labels=eu,name="")+
  scale_y_continuous(expand=expansion(add=0,mult=c(0,.05)))+
  facet_grid(sector~end_use,labeller=labeller(sector=s_label,end_use=eu))+
  theme(axis.text.x = element_blank(),axis.ticks.x=element_blank(),panel.grid.major.x = element_blank())
ggsave(paste0(graph_dir,"/national_annual_2050_by_eu_sector.jpeg"),device = "jpeg",width = 10, height =3,units = "in")

