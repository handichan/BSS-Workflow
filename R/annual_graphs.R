# initialize ---------------------------------------------------------------------
setwd("R")

#install the packages if they're not already installed
packages <- c("tidyverse", "scales")
install.packages(setdiff(packages, rownames(installed.packages())))

#load required packages
library(tidyverse)
library(scales)
theme_set(theme_bw())

# scenarios
scenarios<-c("aeo", "ref", "fossil", "state", "accel", "brk", "dual_switch", "high_switch", "min_switch")
# scenario to get the baseline data from
scenario_for_baseline <- "aeo"

input_dir <- "../scout_tsv" #directory where the tsvs are stored
graph_dir <- "graphs" #directory where the graphs will be written

wide<-data.frame()
# Scout results - output of calc_annual 
for (scen in scenarios){
  if (scen==scenario_for_baseline){
    wide<-bind_rows(wide,read_tsv(paste0(input_dir,"/scout_annual_state_",scen,".tsv")))
  } else{
    wide<-bind_rows(wide,read_tsv(paste0(input_dir,"/scout_annual_state_",scen,".tsv")) %>% filter(turnover!="baseline"))
  }
}

# nice names for the Scout measures
mm<-read_tsv("../map_meas/measure_map.tsv")
mm_long<-pivot_longer(mm %>% select(-c(original_ann:measure_ts)) %>% rename(measure_ann=measure_desc_simple,original_ann=original_desc_simple),
                      names_to="tech_stage",values_to="description",original_ann:measure_ann)



# for nice labeling
# check before running
# Scout scenarios -- every value of "turnover" should be here
# this is the order they'll be shown in facet plots, etc
to<-c(baseline="baseline",
      aeo="AEO 2025",
      ref="Reference",
      fossil="Fossil Favorable",
      stated_policies="Stated Policies",
      mid="Mid",
      high="High",
      state="State and Local Action",
      accel="Accelerated Innovation",
      breakthrough="Breakthrough",
      brk="Breakthrough",
      ineff="Inefficient",
      dual_switch="Dual Switch",
      high_switch="High Switch",
      min_switch="Min Switch"
)
# sector
sec<-c(com="Commercial",res="Residential",all="All Buildings")
# end uses
eu<-c(`Computers and Electronics`="Computers and Electronics",Cooking="Cooking",`Cooling (Equip.)`="Cooling",`Heating (Equip.)`="Heating",
      Lighting="Lighting",Other="Other",Refrigeration="Refrigeration",Ventilation="Ventilation",`Water Heating`="Water Heating")

#fill colors
colors<-c("#1e4c71",	"#377eb8","#b3cde3",
          "#8d0c0d",	"#e41a1c","#fbb4ae",
          "#be9829",	"#ffce3b","#fef488",
          "#5c2d63",	"#984ea3","#decbe4",
          "#be5d00",	"#ff7f00","#fed9a6",
          "#2c6b2a",	"#4daf4a","#ccebc5",
          "#653215",	"#a65628","#cc997f",
          "#5d5d5d",	"#999999","gray80")
h_1<-3.4 #height for one row
h_2<-6 #height for two rows
h_3<-8.6 #height for three rows


# states to show as examples for heating and cooling
# check before running
states<-c("WA","CA","MA","FL")
state_height<-length(states)*1.8


# order the scenarios for plotting
wide<-wide %>% mutate(turnover=factor(turnover,levels=names(to),ordered=T))

# choose scenarios to plot ----------------------------------------------
filename_prefix <- ""
scen_filtered<-c("baseline", scenarios)

# filename_prefix <- "aeo_fossil_state_accel_brk_"
# scen_filtered<-c("aeo","fossil","state","accel","brk")

# filename_prefix <- "aeo_min_dual_high_"
# scen_filtered<-c("aeo","min_switch","dual_switch","high_switch")

#width changeable based on number of scenarios
width<-(1+length(scen_filtered))*2

# annual, national --------------------------------------------------------

print("printing 1")
# area plot by year, scenario, end use, sector
wide %>% filter(fuel=="Electric", turnover %in% scen_filtered) %>% 
  group_by(year,sector,end_use,turnover) %>% 
  summarize(kwh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=kwh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=sec))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors)+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario.jpg"),device = "jpeg",width = width, height =h_2,units = "in")

print("printing 1b")
wide %>% filter(fuel!="Electric", turnover %in% scen_filtered) %>% 
  group_by(year,sector,end_use,turnover) %>% 
  summarize(kwh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=kwh,fill=end_use))+
  geom_area() + 
  facet_grid(sector~turnover,labeller = labeller(turnover=to,sector=sec))+
  scale_y_continuous("TWh",labels=comma_format(),expand=expansion(add=0,mult=c(0,.05)))+ 
  scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
  scale_fill_manual(name="",labels=eu,values=colors)+
  theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_sector_scenario_fossil.jpg"),device = "jpeg",width = width, height =h_2,units = "in")


print("printing 2")
# line plot comparing the scenario totals
wide %>% filter(fuel=="Electric", turnover %in% scen_filtered) %>%
  group_by(turnover,year) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>%
  ggplot(aes(x=year,y=TWh,color=turnover))+
  geom_line()+
  scale_y_continuous(limits=c(0,NA),labels=comma_format())+
  scale_color_manual(values =colors,name="",labels=to)+
  xlab("")
ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_lines.jpeg"),device = "jpeg",width = 4.5, height =h_1,units = "in")



# annual, national by tech type ------------------------------------------------------------

with_shapes<-wide %>% filter(fuel=="Electric", turnover %in% scen_filtered) %>% 
  left_join(mm_long, by=c("meas","end_use"="Scout_end_use","tech_stage","sector"))

with_shapes_agg <-with_shapes  %>% group_by(year,end_use,turnover,sector,description) %>% summarize(TWh=sum(state_ann_kwh)/10^9) %>% ungroup() 


print("printing 3")
#HVAC
for (s in c("com","res")){
  h<-if_else(s=="res",h_2,h_3)
  
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>1) %>% 
    filter((end_use %in% c("Cooling (Equip.)","Heating (Equip.)","Ventilation")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(end_use~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",s,"_hvac.jpeg"),device = "jpeg",width = width, height = h,units = "in")
}


print("printing 4")
#water heating
for (s in c("com","res")){
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>1) %>% 
    filter((end_use %in% c("Water Heating")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",s,"_wh.jpeg"),device = "jpeg",width = width, height =h_1,units = "in")
}

print("printing 5")
#non HVAC, non-WH
for (s in c("com","res")){
  with_shapes_agg%>%  group_by(description) %>% filter(sum(TWh)>1) %>% 
    filter(!(end_use %in% c("Water Heating","Heating (Equip.)","Cooling (Equip.)","Ventilation")),sector==s) %>%
    ggplot(aes(x=year,y=TWh,fill=description)) +
    geom_area()+
    facet_grid(~turnover,labeller = labeller(turnover=to,end_use=eu))+
    scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)),labels=comma_format())+ 
    scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
    guides(fill = guide_legend(nrow = 12))+
    scale_fill_manual(values=colors,name="")+
    theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
  ggsave(paste0(graph_dir,"/",filename_prefix,"national_annual_",s,"_non-mech.jpeg"),device = "jpeg",width = width+2, height =h_1,units = "in")
}



# annual by state and tech type for heating and cooling  ------------------------------------------------------------

print("printing 6")
with_shapes_agg_state <-with_shapes %>% filter(reg %in% states) %>% 
  group_by(year,reg,end_use,turnover,sector,description) %>% 
  summarize(TWh=sum(state_ann_kwh)/10^9) %>%
  ungroup()

for(s in c("res","com")){
  for (u in c("Heating (Equip.)","Cooling (Equip.)")){
    with_shapes_agg_state%>% 
      group_by(description) %>% filter(sum(TWh)>1) %>%
      filter(end_use ==u,sector==s) %>%
      ggplot(aes(x=year,y=TWh,fill=description)) +
      geom_area()+
      facet_grid(reg~turnover,labeller = labeller(turnover=to,end_use=eu),scales="free")+
      scale_y_continuous("TWh",expand=expansion(add=0,mult=c(0,.05)))+ 
      scale_x_continuous(name="",expand=c(0,0),breaks=seq(2030,2050,by=10))+
      scale_fill_manual(values=colors,name="")+
      theme(strip.background = element_blank(),strip.text.y = element_text(angle=-90,size=10),strip.text.x = element_text(size=10))
    ggsave(paste0(graph_dir,"/",filename_prefix,"state_annual_",s,"_",eu[u],".jpeg"),device = "jpeg",width = width, height =state_height,units = "in")
  }
}

