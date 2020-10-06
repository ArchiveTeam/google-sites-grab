dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")

local item_value = os.getenv('item_value')
local item_type = os.getenv('item_type')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local item_value_lower = string.lower(item_value)

local discovered = {}
discovered[item_type .. ":" .. item_value] = true

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

discover_item = function(type_, name, tries)
  if tries == nil then
    tries = 0
  end
  item = type_ .. ':' .. name
  if discovered[item] then
    return true
  end
  io.stdout:write("Discovered item " .. item .. ".\n")
  io.stdout:flush()
  local body, code, headers, status = http.request(
    "http://blackbird-amqp.meo.ws:23038/googlesites-lwi23sd7qjsa/",
    item
  )
  if code == 200 or code == 409 then
    discovered[item] = true
    return true
  elseif code == 404 then
    io.stdout:write("Project key not found.\n")
    io.stdout:flush()
  elseif code == 400 then
    io.stdout:write("Bad format.\n")
    io.stdout:flush()
  else
    io.stdout:write("Could not queue discovered item. Retrying...\n")
    io.stdout:flush()
    if tries == 10 then
      io.stdout:write("Maximum retries reached for sending discovered item.\n")
      io.stdout:flush()
    else
      os.execute("sleep " .. math.pow(2, tries))
      return discover_item(type_, name, tries + 1)
    end
  end
  abortgrab = true
  return false
end

allowed = function(url, parenturl)
  if string.match(url, "'+")
    or string.match(url, "[<>\\%*%$;%^%[%],%(%){}]")
    or string.match(url, "^https?://sites%.google%.com/feeds/revision/")
    or string.match(url, "^https?://sites%.google%.com/feeds/content/[^/]+/[^/]+/batch$")
    or string.match(url, "^https?://accounts%.google%.com/ServiceLogin")
    or (
      item_type == "a"
      and string.match(url, "^https?://sites%.google%.com/a/[^/]+/[^/]+/_/tz$")
    )
    or (
      item_type == "site"
      and (
        string.match(url, "^https?://sites%.google%.com/site/[^/]+/_/tz$")
        or string.match(url, "^https?://sites%.google%.com/a/defaultdomain/[^/]+/_/tz$")
      )
    ) then
    return false
  end

  local tested = {}
  for s in string.gmatch(url, "([^/]+)") do
    if tested[s] == nil then
      tested[s] = 0
    end
    if tested[s] == 6 then
      return false
    end
    tested[s] = tested[s] + 1
  end

  local match = string.match(url, "^https?://sites%.google%.com/site/([a-zA-Z0-9%%%-_%.]+)")
  if not match then
    match = string.match(url, "^https?://sites%.google%.com/a/defaultdomain/([a-zA-Z0-9%%%-_%.]+)")
  end
  if not match then
    match = string.match(url, "^https?://sites%.google%.com/a/([^/]+/[a-zA-Z0-9%%%-_%.]+)")
  end
  if match then
    if string.find(match, "/") then
      discover_item("a", match)
      discover_item("domain", string.match(match, "^([^/]+)/"))
    else
      discover_item("site", match)
    end
  end

--[[  if url == "https://sites.google.com/feeds/content/site/" .. item_value
    or url == "https://sites.google.com/feeds/content/site/" .. item_value .. "?max-results=1000000000"
    or url == "https://sites.google.com/feeds/content/" .. item_value
    or url == "https://sites.google.com/feeds/content/" .. item_value .. "?max-results=1000000000" then
    return true
  end]]

  if string.match(url, "^https?://[^/]*%.googlegroups%.com")
    or string.match(url, "^https?://[^/]*google%.com/url") then
    return true
  end

  prev = nil
  for s in string.gmatch(url, "([a-zA-Z0-9%%%-_%.]+)") do
    if item_type == "site" and string.lower(s) == item_value_lower then
      return true
    elseif item_type == "a" then
      if prev and string.lower(prev .. "/" .. s) == item_value_lower then
        return true
      end
      prev = s
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

--[[  -- These types of static resources have a version number that change frequently
  if string.match(url, "^https?://ssl%.gstatic%.com/sites/p/[a-z0-9]+/") and html then
    return false
  end]]

  if (downloaded[url] ~= true and addedtolist[url] ~= true)
    and (allowed(url, parent["url"]) or html == 0) then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  local function check(urla)
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.gsub(string.match(url, "^(.-)%.?$"), "&amp;", "&")
    if string.match(url_, "attredirects=[0-9]+") then
      check(string.match(url_, "^([^%?]+)%?"))
    end
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
      and allowed(url_, origurl) then
      table.insert(urls, {
        url=url_,
        headers={["Accept-Language"]="en-US,en;q=0.7"}})
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (string.match(newurl, "^https?:\\?/\\?//?/?")
        or string.match(newurl, "^[/\\]")
        or string.match(newurl, "^%./")
        or string.match(newurl, "^[jJ]ava[sS]cript:")
        or string.match(newurl, "^[mM]ail[tT]o:")
        or string.match(newurl, "^vine:")
        or string.match(newurl, "^android%-app:")
        or string.match(newurl, "^ios%-app:")
        or string.match(newurl, "^%${")) then
      check(urlparse.absolute(url, newurl))
    end
  end

  if string.match(url, "^[^%?]+%?.*height=")
    or string.match(url, "^[^%?]+%?.*width=") then
    check(string.match(url, "^([^%?]+)"))
  end

  local a, b = string.match(url, "^(https?://sites%.google%.com)/site/(.+)$")
  if not a or not b then
    a, b = string.match(url, "^(https?://sites%.google%.com)/a/defaultdomain/(.+)$")
  end
  if a and b then
    check(a .. "/a/defaultdomain/" .. b)
    check(a .. "/site/" .. b)
  end

  if string.match(url, "^https?://sites%.google%.com/.+/_/rsrc/.+%?")
    or string.match(url, "^https?://[^/]*%.googlegroups%.com/.+%?") then
    check(string.match(url, "^([^%?]+)%?"))
  end

  if allowed(url, nil) and status_code == 200
    and not string.match(url, "^https?://[^/]*%.googlegroups%.com")
    and not (
      item_type == "site"
      and string.match(url, "^https?://sites%.google%.com/site/[^/]+/_/rsrc/")
    )
    and not (
      item_type == "a"
      and string.match(url, "^https?://sites%.google%.com/a/[^/]+/[^/]+/_/rsrc/")
    ) then
    html = read_file(file)
    if string.match(url, "^https?://sites%.google%.com/site/[^/]+/$")
      or string.match(url, "^https?://sites%.google%.com/a/defaultdomain/[^/]+/$") then
      check("https://sites.google.com/feeds/content/site/" .. item_value .. "?max-results=1000000000")
      check("https://sites.google.com/feeds/content/site/" .. item_value)
      check("https://sites.google.com/feeds/content/defaultdomain/" .. item_value .. "?max-results=1000000000")
      check("https://sites.google.com/feeds/content/defaultdomain/" .. item_value)
    elseif string.match(url, "^https?://sites%.google%.com/a/[^/]+/[^/]+/$") then
      check("https://sites.google.com/feeds/content/" .. item_value .. "?max-results=1000000000")
      check("https://sites.google.com/feeds/content/" .. item_value)
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if string.match(newloc, "^https?://[^/]*google%.com/sorry") then
      io.stdout:write("Got a CAPTCHA.\n")
      io.stdout:flush()
      abortgrab = true
    end
    if string.match(newurl, "/matureConfirm%?") then
      io.stdout:write("18+ cookie not working.\n")
      io.stdout:flush()
      abortgrab = true
    end
    if downloaded[newloc] == true or addedtolist[newloc] == true
      or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if status_code >= 200 and status_code <= 399 then
    downloaded[url["url"]] = true
    downloaded[string.gsub(url["url"], "https?://", "http://")] = true
  end

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  if (status_code >= 401 and status_code ~= 404)
    or status_code  == 0 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 10
    if not allowed(url["url"], nil) then
      maxtries = 1
    end
    if tries >= maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      if maxtries == 1 then
        return wget.actions.EXIT
      else
        return wget.actions.ABORT
      end
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

