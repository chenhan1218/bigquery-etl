CREATE TEMP FUNCTION
  udf_json_mode_last(list ANY TYPE) AS ((
    SELECT
      ANY_VALUE(_value)
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS _offset
    GROUP BY
      TO_JSON_STRING(_value)
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1));
CREATE TEMP FUNCTION
  udf_aggregate_active_addons(active_addons ANY TYPE) AS (ARRAY(
      SELECT
        STRUCT(key, STRUCT(udf_json_mode_last(ARRAY_AGG(value)) AS value))
      FROM
        UNNEST(active_addons)
      GROUP BY
        key));
CREATE TEMP FUNCTION
  udf_boolean_histogram_to_boolean(histogram STRING) AS (
    COALESCE(SAFE_CAST(JSON_EXTRACT_SCALAR(histogram,
          "$.values.1") AS INT64) > 0,
      NOT SAFE_CAST( JSON_EXTRACT_SCALAR(histogram,
          "$.values.0") AS INT64) > 0));
CREATE TEMP FUNCTION udf_extract_count_histogram(histogram STRING) AS (SAFE_CAST(JSON_EXTRACT(histogram, '$.values.0') AS INT64));
CREATE TEMP FUNCTION
  udf_geo_struct(country STRING,
    city STRING,
    geo_subdivision1 STRING,
    geo_subdivision2 STRING) AS ( IF(country IS NULL
      OR country = '??',
      NULL,
      STRUCT(country,
        NULLIF(city,
          '??') AS city,
        NULLIF(geo_subdivision1,
          '??') AS geo_subdivision1,
        NULLIF(geo_subdivision2,
          '??') AS geo_subdivision2)));
CREATE TEMP FUNCTION udf_get_key(map ANY TYPE, k ANY TYPE) AS (
 (
   SELECT key_value.value
   FROM UNNEST(map) AS key_value
   WHERE key_value.key = k
   LIMIT 1
 )
);
CREATE TEMP FUNCTION
  udf_json_extract_int_map (input STRING) AS (ARRAY(
    SELECT
      STRUCT(CAST(SPLIT(entry, ':')[OFFSET(0)] AS INT64) AS key,
             CAST(SPLIT(entry, ':')[OFFSET(1)] AS INT64) AS value)
    FROM
      UNNEST(SPLIT(REPLACE(TRIM(input, '{}'), '"', ''), ',')) AS entry
    WHERE
      LENGTH(entry) > 0 ));
CREATE TEMP FUNCTION
  udf_json_extract_histogram (input STRING) AS (STRUCT(
    CAST(JSON_EXTRACT_SCALAR(input, '$.bucket_count') AS INT64) AS bucket_count,
    CAST(JSON_EXTRACT_SCALAR(input, '$.histogram_type') AS INT64) AS histogram_type,
    CAST(JSON_EXTRACT_SCALAR(input, '$.sum') AS INT64) AS `sum`,
    ARRAY(
      SELECT
        CAST(bound AS INT64)
      FROM
        UNNEST(SPLIT(TRIM(JSON_EXTRACT(input, '$.range'), '[]'), ',')) AS bound) AS `range`,
    udf_json_extract_int_map(JSON_EXTRACT(input, '$.values')) AS `values` ));
CREATE TEMP FUNCTION
  udf_mode_last(list ANY TYPE) AS ((
    SELECT
      _value
    FROM
      UNNEST(list) AS _value
    WITH
    OFFSET
      AS
    _offset
    GROUP BY
      _value
    ORDER BY
      COUNT(_value) DESC,
      MAX(_offset) DESC
    LIMIT
      1 ));
CREATE TEMP FUNCTION
  udf_null_if_empty_list(list ANY TYPE) AS ( IF(ARRAY_LENGTH(list) > 0,
      list,
      NULL) );
--
WITH
  -- normalize client_id and rank by document_id
  numbered_duplicates AS (
  SELECT
    ROW_NUMBER() OVER (PARTITION BY client_id, submission_timestamp, document_id ORDER BY `submission_timestamp` ASC) AS _n,
    * REPLACE(LOWER(client_id) AS client_id)
  FROM
    `moz-fx-data-shared-prod.telemetry_stable.main_v4`
  WHERE
    submission_timestamp = TIMESTAMP(@submission_date)
    AND client_id IS NOT NULL ),
  -- Deduplicating on document_id is necessary to get valid SUM values.
  deduplicated AS (
  SELECT
    * EXCEPT (_n)
  FROM
    numbered_duplicates
  WHERE
    _n = 1 )
SELECT
  DATE(submission_timestamp) as submission_date,
  client_id,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "content")).sum) AS aborts_content_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "gmplugin")).sum) AS aborts_gmplugin_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_abnormal_abort, "plugin")).sum) AS aborts_plugin_sum,
  AVG(ARRAY_LENGTH(environment.addons.active_addons)) AS active_addons_count_mean,
  udf_aggregate_active_addons(ARRAY_CONCAT_AGG(environment.addons.active_addons)) AS active_addons,
  CAST(NULL AS STRING) AS active_experiment_branch, -- deprecated
  CAST(NULL AS STRING) AS active_experiment_id, -- deprecated
  SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.active_ticks") AS INT64)/(3600/5)) AS active_hours_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.addon_compatibility_check_enabled)) AS addon_compatibility_check_enabled,
  udf_mode_last(ARRAY_AGG(application.build_id)) AS app_build_id,
  udf_mode_last(ARRAY_AGG(application.display_version)) AS app_display_version,
  udf_mode_last(ARRAY_AGG(application.name)) AS app_name,
  udf_mode_last(ARRAY_AGG(application.version)) AS app_version,
  udf_json_mode_last(ARRAY_AGG(STRUCT(environment.settings.attribution.source, environment.settings.attribution.medium, environment.settings.attribution.campaign, environment.settings.attribution.content))) AS attribution,
  udf_mode_last(ARRAY_AGG(environment.settings.blocklist_enabled)) AS blocklist_enabled,
  udf_mode_last(ARRAY_AGG(metadata.uri.app_update_channel)) AS channel,
  AVG(TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%a, %d %b %Y %T %Z", metadata.header.date), submission_timestamp, SECOND)) AS client_clock_skew_mean,
  AVG(TIMESTAMP_DIFF(SAFE.PARSE_TIMESTAMP("%FT%R:%E*SZ", creation_date), submission_timestamp, SECOND)) AS client_submission_latency_mean,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.cores)) AS cpu_cores,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.count)) AS cpu_count,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.family)) AS cpu_family,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.cpu.l2cache_kb AS INT64))) AS cpu_l2_cache_kb,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.cpu.l3cache_kb AS INT64))) AS cpu_l3_cache_kb,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.model)) AS cpu_model,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.speed_m_hz)) AS cpu_speed_mhz,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.stepping)) AS cpu_stepping,
  udf_mode_last(ARRAY_AGG(environment.system.cpu.vendor)) AS cpu_vendor,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "content")).sum) AS crashes_detected_content_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "gmplugin")).sum) AS crashes_detected_gmplugin_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "plugin")).sum) AS crashes_detected_plugin_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "main_crash")).sum) as crash_submit_attempt_main_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "content_crash")).sum) as crash_submit_attempt_content_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_attempt, "plugin_crash")).sum) as crash_submit_attempt_plugin_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "main_crash")).sum) as crash_submit_success_main_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "content_crash")).sum) as crash_submit_success_content_sum,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.process_crash_submit_success, "plugin_crash")).sum) as crash_submit_success_plugin_sum,
  -- -- -- --
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine)) AS default_search_engine,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.load_path)) AS default_search_engine_data_load_path,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.name)) AS default_search_engine_data_name,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.origin)) AS default_search_engine_data_origin,
  udf_mode_last(ARRAY_AGG(environment.settings.default_search_engine_data.submission_url)) AS default_search_engine_data_submission_url,
  SUM(  udf_json_extract_histogram(payload.histograms.devtools_toolbox_opened_count).sum) AS devtools_toolbox_opened_count_sum,
  udf_mode_last(ARRAY_AGG(environment.partner.distribution_id)) AS distribution_id,
  udf_mode_last(ARRAY_AGG(environment.settings.e10s_enabled)) AS e10s_enabled,
  udf_mode_last(ARRAY_AGG(environment.build.architecture)) AS env_build_arch,
  udf_mode_last(ARRAY_AGG(environment.build.build_id)) AS env_build_id,
  udf_mode_last(ARRAY_AGG(environment.build.version)) AS env_build_version,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.accept_languages))) AS environment_settings_intl_accept_languages,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.app_locales))) AS environment_settings_intl_app_locales,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.available_locales))) AS environment_settings_intl_available_locales,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.requested_locales))) AS environment_settings_intl_requested_locales,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.system_locales))) AS environment_settings_intl_system_locales,
  udf_json_mode_last(ARRAY_CONCAT_AGG(udf_null_if_empty_list(environment.settings.intl.regional_prefs_locales))) AS environment_settings_intl_regional_prefs_locales,
  ARRAY_AGG((
    SELECT AS STRUCT
      key,
      udf_mode_last(ARRAY_AGG(value.branch)) AS value
    FROM
      UNNEST(environment.experiments) GROUP BY key)) AS experiments,
  AVG(COALESCE(
    payload.processes.parent.scalars.timestamps_first_paint,
    SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.first_paint") AS INT64))) AS first_paint_mean,
  udf_json_mode_last(ARRAY_AGG((
    SELECT
      version
    FROM
      UNNEST(environment.addons.active_plugins),
      UNNEST([STRUCT(SPLIT(version, ".") AS parts)])
    WHERE
      name = "Shockwave Flash"
    ORDER BY
      SAFE_CAST(parts[SAFE_OFFSET(0)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(1)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(2)] AS INT64) DESC,
      SAFE_CAST(parts[SAFE_OFFSET(3)] AS INT64) DESC
    LIMIT
      1
  ))) as flash_version,
  udf_geo_struct(metadata.geo.country, metadata.geo.city, metadata.geo.subdivision1, metadata.geo.subdivision2).*,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.advanced_layers.status)) AS gfx_features_advanced_layers_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.d2d.status)) AS gfx_features_d2d_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.d3d11.status)) AS gfx_features_d3d11_status,
  udf_mode_last(ARRAY_AGG(environment.system.gfx.features.gpu_process.status)) AS gfx_features_gpu_process_status,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_aboutdebugging_opened_count)) AS histogram_parent_devtools_aboutdebugging_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_animationinspector_opened_count)) AS histogram_parent_devtools_animationinspector_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_browserconsole_opened_count)) AS histogram_parent_devtools_browserconsole_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_canvasdebugger_opened_count)) AS histogram_parent_devtools_canvasdebugger_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_computedview_opened_count)) AS histogram_parent_devtools_computedview_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_custom_opened_count)) AS histogram_parent_devtools_custom_opened_count_sum,
  NULL AS histogram_parent_devtools_developertoolbar_opened_count_sum, -- deprecated
  SUM(udf_extract_count_histogram(payload.histograms.devtools_dom_opened_count)) AS histogram_parent_devtools_dom_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_eyedropper_opened_count)) AS histogram_parent_devtools_eyedropper_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_fontinspector_opened_count)) AS histogram_parent_devtools_fontinspector_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_inspector_opened_count)) AS histogram_parent_devtools_inspector_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_jsbrowserdebugger_opened_count)) AS histogram_parent_devtools_jsbrowserdebugger_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_jsdebugger_opened_count)) AS histogram_parent_devtools_jsdebugger_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_jsprofiler_opened_count)) AS histogram_parent_devtools_jsprofiler_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_layoutview_opened_count)) AS histogram_parent_devtools_layoutview_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_memory_opened_count)) AS histogram_parent_devtools_memory_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_menu_eyedropper_opened_count)) AS histogram_parent_devtools_menu_eyedropper_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_netmonitor_opened_count)) AS histogram_parent_devtools_netmonitor_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_options_opened_count)) AS histogram_parent_devtools_options_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_paintflashing_opened_count)) AS histogram_parent_devtools_paintflashing_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_picker_eyedropper_opened_count)) AS histogram_parent_devtools_picker_eyedropper_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_responsive_opened_count)) AS histogram_parent_devtools_responsive_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_ruleview_opened_count)) AS histogram_parent_devtools_ruleview_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_scratchpad_opened_count)) AS histogram_parent_devtools_scratchpad_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_scratchpad_window_opened_count)) AS histogram_parent_devtools_scratchpad_window_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_shadereditor_opened_count)) AS histogram_parent_devtools_shadereditor_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_storage_opened_count)) AS histogram_parent_devtools_storage_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_styleeditor_opened_count)) AS histogram_parent_devtools_styleeditor_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_webaudioeditor_opened_count)) AS histogram_parent_devtools_webaudioeditor_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_webconsole_opened_count)) AS histogram_parent_devtools_webconsole_opened_count_sum,
  SUM(udf_extract_count_histogram(payload.histograms.devtools_webide_opened_count)) AS histogram_parent_devtools_webide_opened_count_sum,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.install_year AS INT64))) AS install_year,
  udf_mode_last(ARRAY_AGG(environment.settings.is_default_browser)) AS is_default_browser,
  udf_mode_last(ARRAY_AGG(environment.system.is_wow64)) AS is_wow64,
  udf_mode_last(ARRAY_AGG(environment.settings.locale)) AS locale,
  udf_mode_last(ARRAY_AGG(environment.system.memory_mb)) AS memory_mb,
  udf_mode_last(ARRAY_AGG(normalized_channel)) AS normalized_channel,
  udf_mode_last(ARRAY_AGG(normalized_os_version)) AS normalized_os_version,
  udf_mode_last(ARRAY_AGG(environment.system.os.name)) AS os,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.service_pack_major AS INT64))) AS os_service_pack_major,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.service_pack_major AS INT64))) AS os_service_pack_minor,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.service_pack_major AS INT64))) AS os_version,
  COUNT(*) AS pings_aggregated_by_this_row,
  AVG((SELECT SAFE_CAST(AVG(value) AS INT64) FROM UNNEST(udf_json_extract_histogram(payload.histograms.places_bookmarks_count).values))) AS places_bookmarks_count_mean,
  AVG((SELECT SAFE_CAST(AVG(value) AS INT64) FROM UNNEST(udf_json_extract_histogram(payload.histograms.places_pages_count).values))) AS places_pages_count_mean,
  SUM(udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_crashes_with_dump, "pluginhang")).sum) AS plugin_hangs_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum) AS plugins_infobar_allow_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum) AS plugins_infobar_block_sum,
  SUM(udf_json_extract_histogram(payload.histograms.plugins_infobar_allow).sum) AS plugins_infobar_shown_sum,
  SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.plugins_notification_user_action, "$.values.1") AS INT64)) AS plugins_notification_shown_sum,
  udf_mode_last(ARRAY_AGG(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.previous_build_id"))) AS previous_build_id,
  UNIX_DATE(DATE(SAFE.TIMESTAMP(ANY_VALUE(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.subsession_start_date"))))) - ANY_VALUE(SAFE_CAST(environment.profile.creation_date AS INT64)) AS profile_age_in_days,
  SAFE.DATE_FROM_UNIX_DATE(ANY_VALUE(SAFE_CAST(environment.profile.creation_date AS INT64))) AS profile_creation_date,
  SUM(udf_json_extract_histogram(payload.histograms.push_api_notify).sum) AS push_api_notify_sum,
  ANY_VALUE(sample_id) AS sample_id,
  udf_mode_last(ARRAY_AGG(environment.settings.sandbox.effective_content_process_level)) AS sandbox_effective_content_process_level,
  SUM(payload.processes.parent.scalars.webrtc_nicer_stun_retransmits + payload.processes.content.scalars.webrtc_nicer_stun_retransmits) AS scalar_combined_webrtc_nicer_stun_retransmits_sum,
  SUM(payload.processes.parent.scalars.webrtc_nicer_turn_401s + payload.processes.content.scalars.webrtc_nicer_turn_401s) AS scalar_combined_webrtc_nicer_turn_401s_sum,
  SUM(payload.processes.parent.scalars.webrtc_nicer_turn_403s + payload.processes.content.scalars.webrtc_nicer_turn_403s) AS scalar_combined_webrtc_nicer_turn_403s_sum,
  SUM(payload.processes.parent.scalars.webrtc_nicer_turn_438s + payload.processes.content.scalars.webrtc_nicer_turn_438s) AS scalar_combined_webrtc_nicer_turn_438s_sum,
  SUM(payload.processes.content.scalars.navigator_storage_estimate_count) AS scalar_content_navigator_storage_estimate_count_sum,
  SUM(payload.processes.content.scalars.navigator_storage_persist_count) AS scalar_content_navigator_storage_persist_count_sum,
  udf_mode_last(ARRAY_AGG(payload.processes.parent.scalars.aushelper_websense_reg_version)) AS scalar_parent_aushelper_websense_reg_version,
  MAX(payload.processes.parent.scalars.browser_engagement_max_concurrent_tab_count) AS scalar_parent_browser_engagement_max_concurrent_tab_count_max,
  MAX(payload.processes.parent.scalars.browser_engagement_max_concurrent_window_count) AS scalar_parent_browser_engagement_max_concurrent_window_count_max,
  SUM(payload.processes.parent.scalars.browser_engagement_tab_open_event_count) AS scalar_parent_browser_engagement_tab_open_event_count_sum,
  SUM(payload.processes.parent.scalars.browser_engagement_total_uri_count) AS scalar_parent_browser_engagement_total_uri_count_sum,
  SUM(payload.processes.parent.scalars.browser_engagement_unfiltered_uri_count) AS scalar_parent_browser_engagement_unfiltered_uri_count_sum,
  MAX(payload.processes.parent.scalars.browser_engagement_unique_domains_count) AS scalar_parent_browser_engagement_unique_domains_count_max,
  AVG(payload.processes.parent.scalars.browser_engagement_unique_domains_count) AS scalar_parent_browser_engagement_unique_domains_count_mean,
  SUM(payload.processes.parent.scalars.browser_engagement_window_open_event_count) AS scalar_parent_browser_engagement_window_open_event_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_node_inspected_count) AS scalar_parent_devtools_accessibility_node_inspected_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_opened_count) AS scalar_parent_devtools_accessibility_opened_count_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_picker_used_count) AS scalar_parent_devtools_accessibility_picker_used_count_sum,
      SUM((SELECT
        SUM(value)
      FROM
        UNNEST(payload.processes.parent.keyed_scalars.devtools_accessibility_select_accessible_for_node))) AS scalar_parent_devtools_accessibility_select_accessible_for_node_sum,
  SUM(payload.processes.parent.scalars.devtools_accessibility_service_enabled_count) AS scalar_parent_devtools_accessibility_service_enabled_count_sum,
  SUM(payload.processes.parent.scalars.devtools_copy_full_css_selector_opened) AS scalar_parent_devtools_copy_full_css_selector_opened_sum,
  SUM(payload.processes.parent.scalars.devtools_copy_unique_css_selector_opened) AS scalar_parent_devtools_copy_unique_css_selector_opened_sum,
  SUM(payload.processes.parent.scalars.devtools_toolbar_eyedropper_opened) AS scalar_parent_devtools_toolbar_eyedropper_opened_sum,
  NULL AS scalar_parent_dom_contentprocess_troubled_due_to_memory_sum, -- deprecated
  SUM(payload.processes.parent.scalars.navigator_storage_estimate_count) AS scalar_parent_navigator_storage_estimate_count_sum,
  SUM(payload.processes.parent.scalars.navigator_storage_persist_count) AS scalar_parent_navigator_storage_persist_count_sum,
  SUM(payload.processes.parent.scalars.storage_sync_api_usage_extensions_using) AS scalar_parent_storage_sync_api_usage_extensions_using_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.search_cohort)) AS search_cohort,
  ((SELECT
      AS STRUCT
      SUM(udf_json_extract_histogram(value).sum) AS search_count_all,
      SUM(IF(SUBSTR(_key, pos) = "abouthome",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_abouthome,
      SUM(IF(SUBSTR(_key, pos) = "contextmenu",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_contextmenu,
      SUM(IF(SUBSTR(_key, pos) = "newtab",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_newtab,
      SUM(IF(SUBSTR(_key, pos) = "searchbar",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_searchbar,
      SUM(IF(SUBSTR(_key, pos) = "system",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_system,
      SUM(IF(SUBSTR(_key, pos) = "urlbar",
          udf_json_extract_histogram(value).sum,
          0)) AS search_count_urlbar
    FROM
      UNNEST(ARRAY_CONCAT_AGG(payload.keyed_histograms.search_counts)),
      UNNEST([REPLACE(key, 'in-content.', 'in-content:')]) AS _key,
      UNNEST([LENGTH(REGEXP_EXTRACT(_key, '.+[.].'))]) AS pos
    WHERE
      SUBSTR(_key, pos) IN ("abouthome",
        "contextmenu",
        "newtab",
        "searchbar",
        "system",
        "urlbar"))).*,
  AVG(SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.simple_measurements.session_restored") AS INT64)) AS session_restored_mean,
  COUNTIF(SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.subsession_counter") AS INT64) = 1) AS sessions_started_on_this_day,
  SUM(  udf_json_extract_histogram(udf_get_key(payload.keyed_histograms.subprocess_kill_hard, "shut_down_kill")).sum) AS shutdown_kill_sum,
  SUM(payload.info.subsession_length/NUMERIC '3600') AS subsession_hours_sum,
  SUM((SELECT SUM(value) FROM UNNEST(udf_json_extract_histogram(payload.histograms.ssl_handshake_result).values) WHERE key BETWEEN 1 AND 671)) AS ssl_handshake_result_failure_sum,
  SUM(SAFE_CAST(JSON_EXTRACT_SCALAR(payload.histograms.ssl_handshake_result, "$.values.0") AS INT64)) AS ssl_handshake_result_success_sum,
  udf_mode_last(ARRAY_AGG(  udf_boolean_histogram_to_boolean(payload.histograms.weave_configured))) AS sync_configured,
  udf_enum_histogram_to_count(payload.histograms.weave_device_count_desktop) AS sync_count_desktop,
  AVG(udf_enum_histogram_to_count(payload.histograms.weave_device_count_desktop)) AS sync_count_desktop_mean,
  AVG(udf_enum_histogram_to_count(payload.histograms.weave_device_count_mobile)) AS sync_count_mobile_mean,
  SUM(udf_enum_histogram_to_count(payload.histograms.weave_device_count_desktop)) AS sync_count_desktop_sum,
  SUM(udf_enum_histogram_to_count(payload.histograms.weave_device_count_mobile)) AS sync_count_mobile_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.telemetry_enabled)) AS telemetry_enabled,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(JSON_EXTRACT_SCALAR(additional_properties, "$.payload.info.timezone_offset") AS INT64))) AS timezone_offset,
  CAST(NULL AS NUMERIC) AS total_hours_sum,
  udf_mode_last(ARRAY_AGG(environment.settings.update.auto_download)) AS update_auto_download,
  udf_mode_last(ARRAY_AGG(environment.settings.update.channel)) AS update_channel,
  udf_mode_last(ARRAY_AGG(environment.settings.update.enabled)) AS update_enabled,
  udf_mode_last(ARRAY_AGG(application.vendor)) AS vendor,
  SUM(udf_json_extract_histogram(payload.histograms.web_notification_shown).sum) AS web_notification_shown_sum,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.windows_build_number AS INT64))) AS windows_build_number,
  udf_mode_last(ARRAY_AGG(SAFE_CAST(environment.system.os.windows_ubr AS INT64))) AS windows_ubr
FROM
  deduplicated
GROUP BY
  client_id,
  submission_timestamp
