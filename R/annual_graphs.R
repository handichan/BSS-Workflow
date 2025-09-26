# ---- working dir & packages ---------------------------------------------------
setwd("R")

# install the packages if not already installed
packages <- c("tidyverse", "scales")
install.packages(setdiff(packages, rownames(installed.packages())))

# load required packages
library(tidyverse)
library(scales)
theme_set(theme_bw())

# ---- scenarios & IO -----------------------------------------------------------
# All scenarios available in your TSVs (unchanged)
scenarios <- c("brk", "fossil", "accel", "aeo", "ref", "state",
               "dual_switch", "high_switch", "min_switch")

# scenario used to keep the baseline rows (unchanged)
scenario_for_baseline <- "aeo"

# I/O locations (unchanged names)
input_dir   <- "../scout_tsv"   # where TSVs live
filename_prefix <- ""
graph_dir   <- "graphs/annual_results"        # where graphs are written

# ---- read data ----------------------------------------------------------------
wide <- data.frame()
# Scout results - output of calc_annual (unchanged logic)
for (scen in scenarios){
  if (scen == scenario_for_baseline){
    wide <- bind_rows(
      wide,
      read_tsv(paste0(input_dir, "/scout_annual_state_", scen, ".tsv"))
    )
  } else{
    wide <- bind_rows(
      wide,
      read_tsv(paste0(input_dir, "/scout_annual_state_", scen, ".tsv")) %>%
        filter(turnover != "baseline")
    )
  }
}

# nice names for the Scout measures (unchanged)
mm <- read_tsv("../map_meas/measure_map.tsv")
mm_long <- pivot_longer(
  mm %>% select(-c(original_ann:measure_ts)) %>%
    rename(measure_ann = measure_desc_simple,
           original_ann = original_desc_simple),
  names_to = "tech_stage",
  values_to = "description",
  original_ann:measure_ann
)

# ---- labellers & colors (unchanged) -------------------------------------------
# labels for turnover (scenario) facets — keep as-is
to <- c(
  baseline      = "AEO 2025",
  aeo           = "AEO 2025 BTB performance",
  ref           = "Reference",
  stated_policies = "Stated Policies",
  state         = "State and Local Action",
  mid           = "Mid",
  high          = "High",
  accel         = "Accelerated\nInnovation",
  fossil        = "Fossil Favorable",
  breakthrough  = "Breakthrough",
  brk           = "Breakthrough",
  ineff         = "Inefficient",
  dual_switch   = "Dual Switch",
  high_switch   = "High Switch",
  min_switch    = "Min Switch"
)

# sector labels
sec <- c(com = "Commercial", res = "Residential", all = "All Buildings")

# end-use labels
eu <- c(
  `Computers and Electronics` = "Computers and Electronics",
  Cooking = "Cooking",
  `Cooling (Equip.)` = "Cooling",
  `Heating (Equip.)` = "Heating",
  Lighting = "Lighting",
  Other = "Other",
  Refrigeration = "Refrigeration",
  Ventilation = "Ventilation",
  `Water Heating` = "Water Heating"
)

# fill colors (unchanged palette)
colors <- c(
  "#1e4c71", "#377eb8", "#b3cde3",
  "#8d0c0d", "#e41a1c", "#fbb4ae",
  "#be9829", "#ffce3b", "#fef488",
  "#5c2d63", "#984ea3", "#decbe4",
  "#be5d00", "#ff7f00", "#fed9a6",
  "#2c6b2a", "#4daf4a", "#ccebc5",
  "#653215", "#a65628", "#cc997f",
  "#5d5d5d", "#999999", "gray80"
)

# ---- states to show (unchanged) -----------------------------------------------
# example states for heating/cooling state panels
states <- c("WA","CA","MA","FL")

# ---- scenario-set helper & plotting -------------------------------------------
# Three sets (ALWAYS start with "AEO 2025" = turnover "baseline"):
#   S1: AEO 2025, Fossil Favorable, State and Local Action, Breakthrough
#   S2: AEO 2025, Reference, Accelerated Innovation
#   S3: AEO 2025, Dual Switch, High Switch, Min Switch
set1_codes <- c("baseline","fossil","state","brk")
set2_codes <- c("baseline","ref","accel")
set3_codes <- c("baseline","dual_switch","high_switch","min_switch")
setall_codes <- c("baseline","fossil","state","brk","ref","accel","dual_switch","high_switch","min_switch")

# factor & filter to a given set while preserving desired order
.make_factor <- function(df, codes) {
  df %>%
    filter(turnover %in% codes) %>%
    mutate(turnover = factor(turnover, levels = codes, ordered = TRUE))
}


.make_all_plots_for <- function(df_in, suffix_tag, mm_long_ref, state_vec, to_lab, sec_lab, eu_lab, colors_vec) {
  # dynamic sizes for the set
  scen_levels <- levels(df_in$turnover)
  nscen <- length(scen_levels)
  width_set <- (1 + nscen) * 1.8
  state_height <- length(state_vec) * 1.4

  # ----- 2) national totals lines ---------------------------------------------
  message(paste0("printing national lines ", suffix_tag))
  df_in %>% filter(fuel=="Electric") %>%
    group_by(turnover, year) %>%
    summarize(TWh = sum(state_ann_kwh)/1e9, .groups="drop") %>%
    ggplot(aes(x=year, y=TWh, color=turnover)) +
    geom_line() +
    scale_y_continuous(limits=c(0, NA), labels=comma_format()) +
    scale_color_manual(values = colors_vec, name="", labels = to_lab) +
    xlab("")
  ggsave(paste0(graph_dir,"/national_annual_lines", suffix_tag, ".jpeg"),
         device="jpeg", width=4.5, height=3, units="in")
}


# master plotting function for a single set (adds filename suffix)
.make_split_plots_for <- function(df_in, suffix_tag, mm_long_ref, state_vec, to_lab, sec_lab, eu_lab, colors_vec) {

  # dynamic sizes for the set
  scen_levels <- levels(df_in$turnover)
  nscen <- length(scen_levels)
  width_set <- (1 + nscen) * 1.8
  state_height <- length(state_vec) * 1.8

  # ----- 1) national, Electric -------------------------------------------------
  message(paste0("printing national electric ", suffix_tag))
  df_in %>% filter(fuel=="Electric") %>%
    group_by(year, sector, end_use, turnover) %>%
    summarize(kwh = sum(state_ann_kwh)/1e9, .groups="drop") %>%
    ggplot(aes(x=year,y=kwh,fill=end_use)) +
    geom_area() +
    facet_grid(sector ~ turnover,
               labeller = labeller(turnover = to_lab, sector = sec_lab)) +
    scale_y_continuous("TWh", labels = comma_format(),
                       expand = expansion(add=0, mult=c(0,.05))) +
    scale_x_continuous(name = "", expand=c(0,0), breaks = seq(2030,2050,by=10)) +
    scale_fill_manual(name = "", labels = eu_lab, values = colors_vec) +
    theme(strip.background = element_blank(),
          strip.text.y = element_text(angle=-90, size=10),
          strip.text.x = element_text(size=10))
  ggsave(paste0(graph_dir,"/national_annual_sector_scenario", suffix_tag, ".jpeg"),
         device="jpeg", width=width_set, height=5, units="in")

  # ----- 1b) national, non-Electric -------------------------------------------
  message(paste0("printing national non-electric ", suffix_tag))
  df_in %>% filter(fuel!="Electric") %>%
    group_by(year, sector, end_use, turnover) %>%
    summarize(kwh = sum(state_ann_kwh)/1e9, .groups="drop") %>%
    ggplot(aes(x=year,y=kwh,fill=end_use)) +
    geom_area() +
    facet_grid(sector ~ turnover,
               labeller = labeller(turnover = to_lab, sector = sec_lab)) +
    scale_y_continuous("TWh", labels = comma_format(),
                       expand = expansion(add=0, mult=c(0,.05))) +
    scale_x_continuous(name = "", expand=c(0,0), breaks = seq(2030,2050,by=10)) +
    scale_fill_manual(name = "", labels = eu_lab, values = colors_vec) +
    theme(strip.background = element_blank(),
          strip.text.y = element_text(angle=-90, size=10),
          strip.text.x = element_text(size=10))
  ggsave(paste0(graph_dir,"/national_annual_sector_scenario_fossil", suffix_tag, ".jpeg"),
         device="jpeg", width=width_set, height=5, units="in")


  # ----- 3) national by tech type ---------------------------------------------
  with_shapes <- df_in %>%
    filter(fuel=="Electric") %>%
    left_join(mm_long_ref,
              by=c("meas",
                   "end_use" = "Scout_end_use",
                   "tech_stage",
                   "sector"))

  with_shapes_agg <- with_shapes %>%
    group_by(year, end_use, turnover, sector, description) %>%
    summarize(TWh = sum(state_ann_kwh)/1e9, .groups="drop")

  # 3a) HVAC (Cooling/Heating/Ventilation)
  message(paste0("printing hvac ", suffix_tag))
  for (s in c("com","res")) {
    h <- ifelse(s=="res", 5, 5)
    with_shapes_agg %>%
      group_by(description) %>% filter(sum(TWh) > 1) %>% ungroup() %>%
      filter(end_use %in% c("Cooling (Equip.)","Heating (Equip.)","Ventilation"),
             sector == s) %>%
      ggplot(aes(x=year, y=TWh, fill=description)) +
      geom_area() +
      facet_grid(end_use ~ turnover,
                 labeller = labeller(turnover = to_lab, end_use = eu_lab)) +
      scale_y_continuous("TWh",
                         labels = comma_format(),
                         expand = expansion(add=0, mult=c(0,.05))) +
      scale_x_continuous(name="", expand=c(0,0), breaks=seq(2030,2050,by=10)) +
      scale_fill_manual(values = colors_vec, name = "") +
      theme(strip.background = element_blank(),
            strip.text.y = element_text(angle=-90, size=10),
            strip.text.x = element_text(size=10))
    ggsave(paste0(graph_dir,"/national_annual_", s, "_hvac", suffix_tag, ".jpeg"),
           device="jpeg", width=width_set, height=h, units="in")
  }

  # 3b) Water Heating
  message(paste0("printing water heating ", suffix_tag))
  for (s in c("com","res")) {
    with_shapes_agg %>%
      group_by(description) %>% filter(sum(TWh) > 1) %>% ungroup() %>%
      filter(end_use == "Water Heating", sector == s) %>%
      ggplot(aes(x=year, y=TWh, fill=description)) +
      geom_area() +
      facet_grid(~ turnover, labeller = labeller(turnover = to_lab, end_use = eu_lab)) +
      scale_y_continuous("TWh",
                         labels = comma_format(),
                         expand = expansion(add=0, mult=c(0,.05))) +
      scale_x_continuous(name="", expand=c(0,0), breaks=seq(2030,2050,by=10)) +
      scale_fill_manual(values = colors_vec, name = "") +
      theme(strip.background = element_blank(),
            strip.text.y = element_text(angle=-90, size=10),
            strip.text.x = element_text(size=10))
    ggsave(paste0(graph_dir,"/national_annual_", s, "_wh", suffix_tag, ".jpeg"),
           device="jpeg", width=width_set, height=4/1.5, units="in")
  }

  # 3c) Non-HVAC, non-WH
  message(paste0("printing non-mech ", suffix_tag))
  for (s in c("com","res")) {
    w_nonmech <- ifelse(s=="res", (2 + nscen) * 1.8, width_set)
    with_shapes_agg %>%
      group_by(description) %>% filter(sum(TWh) > 1) %>% ungroup() %>%
      filter(!(end_use %in% c("Water Heating","Heating (Equip.)","Cooling (Equip.)","Ventilation")),
             sector == s) %>%
      ggplot(aes(x=year, y=TWh, fill=description)) +
      geom_area() +
      facet_grid(~ turnover, labeller = labeller(turnover = to_lab, end_use = eu_lab)) +
      scale_y_continuous("TWh",
                         labels = comma_format(),
                         expand = expansion(add=0, mult=c(0,.05))) +
      scale_x_continuous(name="", expand=c(0,0), breaks=seq(2030,2050,by=10)) +
      guides(fill = guide_legend(nrow = 12)) +
      scale_fill_manual(values = colors_vec, name = "") +
      theme(strip.background = element_blank(),
            strip.text.y = element_text(angle=-90, size=10),
            strip.text.x = element_text(size=10))
    ggsave(paste0(graph_dir,"/national_annual_", s, "_non-mech", suffix_tag, ".jpeg"),
           device="jpeg", width=w_nonmech, height=4/1.15, units="in")
  }

  # ----- 4) state x tech type: Heating & Cooling (facets by turnover, example you shared) ---
  message(paste0("printing state heating/cooling ", suffix_tag))
  with_shapes_agg_state <- with_shapes %>%
    filter(reg %in% state_vec) %>%
    group_by(year, reg, end_use, turnover, sector, description) %>%
    summarize(TWh = sum(state_ann_kwh)/1e9, .groups="drop")

  for (s in c("res","com")) {
    for (u in c("Heating (Equip.)","Cooling (Equip.)")) {
      with_shapes_agg_state %>%
        group_by(description) %>% filter(sum(TWh) > 1) %>% ungroup() %>%
        filter(end_use == u, sector == s) %>%
        ggplot(aes(x=year, y=TWh, fill=description)) +
        geom_area() +
        facet_grid(reg ~ turnover,
                   labeller = labeller(turnover = to_lab, end_use = eu_lab),
                   scales = "free") +
        scale_y_continuous("TWh",
                           expand = expansion(add=0, mult=c(0,.05))) +
        scale_x_continuous(name="", expand=c(0,0), breaks=seq(2030,2050,by=10)) +
        scale_fill_manual(values = colors_vec, name = "") +
        theme(strip.background = element_blank(),
              strip.text.y = element_text(angle=-90, size=10),
              strip.text.x = element_text(size=10))
      ggsave(
        paste0(graph_dir,"/state_annual_", s, "_", eu_lab[u], suffix_tag, ".jpeg"),
        device="jpeg", width=width_set, height=state_height, units="in"
      )
    }
  }
}

# ---- run all three sets -------------------------------------------------------
wide_S1 <- .make_factor(wide, set1_codes)
wide_S2 <- .make_factor(wide, set2_codes)
wide_S3 <- .make_factor(wide, set3_codes)

.make_split_plots_for(wide_S1, "_S1", mm_long, states, to, sec, eu, colors)   # AEO + Fossil + State + Breakthrough
.make_split_plots_for(wide_S2, "_S2", mm_long, states, to, sec, eu, colors)   # AEO + Reference + Accelerated Innovation
.make_split_plots_for(wide_S3, "_S3", mm_long, states, to, sec, eu, colors)   # AEO + Dual + High + Min Switch



wide_all <- .make_factor(wide, setall_codes)
.make_all_plots_for(wide_all, "_all", mm_long, states, to, sec, eu, colors)