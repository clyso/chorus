package ui

import (
	"context"
	"fmt"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	pb "github.com/clyso/chorus/proto/gen/go/chorus"
	"github.com/clyso/chorus/tools/chorctl/internal/api"
	"google.golang.org/protobuf/types/known/emptypb"
	"sort"
	"time"
)

var (
	//pgBar       = progress.New(progress.WithGradient("#dabe95", "#63b6bc"), progress.WithWidth(30))
	accentCol      = lipgloss.Color("30")  //lipgloss.Color("#dabe95")
	borderCol      = lipgloss.Color("102") //lipgloss.Color("#7D56F4")
	selTextCol     = lipgloss.Color("229") //lipgloss.Color("#7D56F4")
	headerStyle    = lipgloss.NewStyle().Border(lipgloss.ThickBorder(), false, true, false, true).Bold(true).BorderForeground(borderCol).Background(accentCol)
	subHeaderStyle = lipgloss.NewStyle().Foreground(borderCol).Border(lipgloss.ThickBorder(), false, true, true, true).BorderForeground(borderCol)
)

var (
	minHeight = 12
	height    = minHeight

	minWidth = 90
	width    = minWidth
	maxWidth = 120
)

func New(ctx context.Context, client pb.ChorusClient) tea.Model {
	model := &UI{
		client:   client,
		ctx:      ctx,
		selected: "",
		table:    nil,
		events:   make(chan tea.Msg),
		spinner:  spinner.New(spinner.WithSpinner(spinner.Points), spinner.WithStyle(lipgloss.NewStyle().Foreground(borderCol).AlignHorizontal(lipgloss.Center))),
	}
	return model
}

var _ tea.Model = &UI{}

type UI struct {
	client pb.ChorusClient
	ctx    context.Context

	storages []*pb.Storage
	main     *pb.Storage
	err      error

	data     []*pb.Replication
	selected string
	table    *table.Model
	spinner  spinner.Model
	events   chan tea.Msg
}

func (u *UI) Init() tea.Cmd {
	return tea.Batch(u.loadStorages, u.loadMigrations, u.spinner.Tick, u.waitEvent)
}

func (u *UI) loadStorages() tea.Msg {
	res, err := u.client.GetStorages(u.ctx, &emptypb.Empty{})
	if err != nil {
		return errMsg(err)
	}
	return storMsg{storages: res.Storages}
}

func (u *UI) loadMigrations() tea.Msg {
	go func() {
		ticker := time.NewTicker(time.Second * 2)
		defer ticker.Stop()
		for {
			select {
			case <-u.ctx.Done():
				return
			case <-ticker.C:
				res, err := u.client.ListReplications(u.ctx, &emptypb.Empty{})
				if err != nil {
					u.events <- errMsg(err)
					return
				}
				sort.Slice(res.Replications, func(i, j int) bool {
					return res.Replications[i].CreatedAt.AsTime().After(res.Replications[j].CreatedAt.AsTime())
				})
				u.events <- migrationsUpdMsg{m: res.Replications}
			}
		}
	}()
	return u.waitEvent
}

type migrationsUpdMsg struct {
	m []*pb.Replication
}

type errMsg error

type storMsg struct {
	storages []*pb.Storage
}

func (u *UI) waitEvent() tea.Msg {
	return <-u.events
}

func (u *UI) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	// Make sure these keys always quit
	if msg, ok := msg.(tea.KeyMsg); ok {
		k := msg.String()
		switch k {
		case "q", "esc", "ctrl+c":
			if u.table == nil {
				return u, tea.Quit
			}
			if u.selected != "" {
				u.selected = ""
				u.updateTable(true)
				return u, nil
			}
			if u.selected == "" {
				return u, tea.Quit
			}

		case "enter":
			//if u.table == nil {
			//	return u, nil
			//}
			//if u.selected == "" {
			//	u.selected = u.table.SelectedRow()[0]
			//	u.updateTable(true)
			//	return u, nil
			//}
		case "backspace":
			if u.table == nil {
				return u, nil
			}
			if u.selected != "" {
				u.selected = ""
				u.updateTable(true)
				return u, nil
			}
		}
	}
	var cmd tea.Cmd
	switch msg := msg.(type) {
	case spinner.TickMsg:
		if u.table != nil {
			return u, nil
		}
		u.spinner, cmd = u.spinner.Update(msg)
		return u, cmd
	case tea.WindowSizeMsg:
		width = max(minWidth, msg.Width)
		width = min(width, maxWidth)
		height = max(minHeight, msg.Height)
	case errMsg:
		u.err = msg
		cmd = tea.Println("err msg", u.err)
	case storMsg:
		u.storages = nil
		for _, stor := range msg.storages {
			s := stor
			if s.IsMain {
				u.main = s
				continue
			}
			u.storages = append(u.storages, s)
		}
		cmd = tea.Println("updated storages", len(u.storages))
	case migrationsUpdMsg:
		//u.addNotStartedMigrations(msg.m)
		u.data = msg.m
		cmd = u.waitEvent
		u.updateTable(false)
		cmd = tea.Batch(cmd, tea.Println("updated migrations", len(u.data)))
	default:
		if u.table != nil {
			tab, tabCmd := u.table.Update(msg)
			u.table = &tab
			cmd = tabCmd
		}
	}
	return u, cmd
}

//func (u *UI) addNotStartedMigrations(migrations []*pb.Migration) {
//	if len(migrations) == len(u.storages) {
//		return
//	}
//	for _, stor := range u.storages {
//		presented := false
//		for _, m := range migrations {
//			if stor.Name == m.ToStorage {
//				presented = true
//				break
//			}
//		}
//		if presented {
//			continue
//		}
//		migrations = append(migrations, &pb.Migration{
//			FromStorage: u.main.Name,
//			ToStorage:   stor.Name,
//		})
//	}
//}

func (u *UI) updateTable(changeSelection bool) {
	var columns []table.Column
	var rows []table.Row
	if u.selected == "" {
		maxLen := make([]int, 7)
		rows = make([]table.Row, len(u.data))
		for i := range u.data {
			d := u.data[i]
			p := 0.0
			if d.InitBytesListed != 0 {
				p = float64(d.InitBytesDone) / float64(d.InitBytesListed)
			}
			bytes := fmt.Sprintf("%s/%s", api.ByteCountIEC(d.InitBytesDone), api.ByteCountIEC(d.InitBytesListed))
			objects := fmt.Sprintf("%d/%d", d.InitObjDone, d.InitObjListed)
			events := fmt.Sprintf("%d/%d", d.EventsDone, d.Events)

			rows[i] = table.Row{fmt.Sprintf("%s:%s:%s->%s", d.User, d.Bucket, d.From, d.To), api.ToPercentage(p), bytes, objects, events, fmt.Sprintf("%v", d.IsPaused), api.DateToAge(d.CreatedAt)}
			updateLen(maxLen, rows[i])
		}
		columns = []table.Column{
			{Title: "Name", Width: maxLen[0]},
			{Title: "Progress", Width: maxLen[1]},
			{Title: "Bytes", Width: maxLen[2]},
			{Title: "Objects", Width: maxLen[3]},
			{Title: "Events", Width: maxLen[4]},
			{Title: "Paused", Width: maxLen[5]},
			{Title: "Age", Width: maxLen[6]},
		}
		columnsLen(maxLen, columns)
	} else {
		//maxLen := make([]int, 6)
		//for _, d := range u.data {
		//	if u.selected != d.ToStorage+":"+d.User {
		//		continue
		//	}
		//	rows = make([]table.Row, len(d.Buckets))
		//	for i, b := range d.Buckets {
		//		p := 0.0
		//		if b.ObjBytesListed != 0 {
		//			p = float64(b.ObjBytesDone) / float64(b.ObjBytesListed)
		//		}
		//		pg := api.ToPercentage(p)
		//		bytes := fmt.Sprintf("%s/%s", api.ByteCountIEC(uint64(b.ObjBytesDone)), api.ByteCountIEC(uint64(b.ObjBytesListed)))
		//		objects := fmt.Sprintf("%d/%d", b.ObjDone, b.ObjListed)
		//		rows[i] = table.Row{b.Name, pg, bytes, objects, api.DateToStr(b.StartedAt), api.MigrationBucketDuration(b)}
		//		updateLen(maxLen, rows[i])
		//	}
		//	break
		//}
		//columns = []table.Column{
		//	{Title: "Bucket", Width: maxLen[0]},
		//	{Title: "Progress", Width: maxLen[1]},
		//	{Title: "Bytes", Width: maxLen[2]},
		//	{Title: "Objects", Width: maxLen[3]},
		//	{Title: "StartedAt", Width: maxLen[4]},
		//	{Title: "Duration", Width: maxLen[5]},
		//}
		//columnsLen(maxLen, columns)
	}
	if u.table == nil || changeSelection {
		tab := table.New(table.WithFocused(u.selected == ""), table.WithColumns(columns), table.WithRows(rows))
		s := table.DefaultStyles()
		s.Header = s.Header.
			BorderStyle(lipgloss.NormalBorder()).
			BorderForeground(borderCol).
			BorderBottom(true).
			AlignHorizontal(lipgloss.Center).
			Bold(false)
		if u.selected == "" {
			s.Selected = s.Selected.
				Foreground(selTextCol).
				Background(accentCol).
				Bold(false)
		} else {
			s.Selected = s.Selected.
				Foreground(lipgloss.NewStyle().GetForeground()).
				Background(lipgloss.NewStyle().GetBackground()).
				Bold(false)
		}
		tab.SetStyles(s)
		u.table = &tab
		u.table.SetHeight(len(u.table.Rows()))
	} else {
		u.table.SetColumns(columns)
		u.table.SetRows(rows)
		u.table.SetHeight(len(u.table.Rows()))
	}

}

func updateLen(lens []int, row table.Row) {
	for i := 0; i < len(row); i++ {
		lens[i] = max(lens[i], len([]rune(row[i])))
	}
}
func columnsLen(lens []int, columns []table.Column) {
	for i := range columns {
		columns[i].Width = max(lens[i], len(columns[i].Title))
		if i != len(columns)-1 {
			columns[i].Width += 1
		}
	}
}

func (u *UI) View() string {
	style := headerStyle.Width(width)
	header := style.Width(width).Render("Chorus migrations dashboard")
	w := style.GetWidth()

	m := "<empty>"
	if u.main != nil {
		m = fmt.Sprintf("%s (%s) - %s", u.main.Name, u.main.Provider, u.main.Address)
	}
	m = lipgloss.NewStyle().Width(w / 2).AlignHorizontal(lipgloss.Right).Render(m)
	sh := subHeaderStyle.Copy().Width(w).Render(lipgloss.JoinHorizontal(lipgloss.Center, lipgloss.NewStyle().Width(w/2).Render("From main storage:"), m))

	tab := u.spinner.View()
	if u.table != nil {
		tab = u.table.View()
	}
	if u.err != nil {
		tab = u.err.Error()
	}
	return lipgloss.JoinVertical(lipgloss.Top, header, sh, tab)
}
