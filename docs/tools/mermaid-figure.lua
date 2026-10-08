-- Replace ```mermaid code blocks with a pre-rendered PNG so pandoc can build a PDF.
--
-- A block is named by an HTML comment immediately before it:
--     <!-- figure: v2_relay_architecture -->
--     ```mermaid
--     ...
--     ```
-- and resolves to figures/<name>.png. Unnamed blocks resolve to
-- figures/<figure_stem>-<N>.png (figure_stem from metadata, default "figure").
-- GitHub ignores the comment and renders the fence natively.

local function figure_name(block)
  if block and block.t == "RawBlock" then
    return block.text:match("<!%-%-%s*figure:%s*([%w_%-%.]+)")
  end
  return nil
end

function Pandoc(doc)
  local stem = doc.meta.figure_stem and pandoc.utils.stringify(doc.meta.figure_stem) or "figure"
  local out = {}
  local count = 0
  local blocks = doc.blocks
  for i, block in ipairs(blocks) do
    if block.t == "CodeBlock" and block.classes:includes("mermaid") then
      count = count + 1
      local name = figure_name(blocks[i - 1]) or string.format("%s-%d", stem, count)
      local caption = blocks[i - 1] and blocks[i - 1].t == "RawBlock"
        and blocks[i - 1].text:match("caption:%s*(.-)%s*%-%->") or nil
      local inlines = caption and pandoc.read(caption, "markdown").blocks[1].content or {}
      local img = pandoc.Image({}, "figures/" .. name .. ".png", "", { width = "85%" })
      table.insert(out, pandoc.Figure({ pandoc.Plain({ img }) }, { long = { pandoc.Plain(inlines) } }, { id = "fig:" .. name }))
    elseif figure_name(block) then
      -- drop the naming comment
    else
      table.insert(out, block)
    end
  end
  doc.blocks = out
  return doc
end

-- Pipe tables without explicit widths are emitted as fixed-width tabulars that can
-- overflow the page. Give any table wider than ~70 characters proportional column
-- widths based on its longest cell per column, so LaTeX wraps the cells.
local function cell_length(cell)
  return #pandoc.utils.stringify(cell.contents)
end

function Table(tbl)
  local n = #tbl.colspecs
  if n == 0 then return nil end
  local longest = {}
  for i = 1, n do longest[i] = 0 end
  local rows = {}
  for _, row in ipairs(tbl.head.rows) do table.insert(rows, row) end
  for _, body in ipairs(tbl.bodies) do
    for _, row in ipairs(body.body) do table.insert(rows, row) end
  end
  for _, row in ipairs(rows) do
    for i, cell in ipairs(row.cells) do
      if i <= n then longest[i] = math.max(longest[i], cell_length(cell)) end
    end
  end
  local total = 0
  for i = 1, n do total = total + longest[i] end
  if total <= 70 then return nil end
  -- Clamp so one very long column cannot starve the others.
  local weights, sum = {}, 0
  for i = 1, n do
    weights[i] = math.max(math.min(longest[i], 60), 10)
    sum = sum + weights[i]
  end
  for i = 1, n do
    local align = tbl.colspecs[i][1]
    tbl.colspecs[i] = { align, weights[i] / sum }
  end
  return tbl
end
