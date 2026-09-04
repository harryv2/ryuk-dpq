package integration

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cucumber/godog"
	"github.com/cucumber/godog/colors"

	"github.com/harryv2/ryuk-dpq/integration/client"
	"github.com/harryv2/ryuk-dpq/integration/cluster"
)

// The whole stack is started once and shared. Scenarios keep out of each
// other's way by using a queue name of their own rather than by restarting
// Docker between them, which would make the suite take an hour.
var (
	stack  *cluster.Cluster
	nodes  = flag.Int("nodes", 3, "how many queue nodes to start")
	keepUp = flag.Bool("keep-up", false, "leave the stack running after the suite")
	opts   = godog.Options{Output: colors.Colored(os.Stdout), Format: "pretty", Strict: true}
)

func TestMain(m *testing.M) {
	flag.Parse()
	opts.Paths = []string{"features"}
	// godog binds its own flags to pflag, which "go test" does not parse, so
	// the tag filter comes from the environment instead.
	opts.Tags = os.Getenv("RYUK_IT_TAGS")

	if testing.Short() {
		fmt.Println("skipping integration suite: -short")
		os.Exit(0)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	var err error
	if stack, err = cluster.New(); err != nil {
		fmt.Fprintln(os.Stderr, "cannot locate the compose file:", err)
		os.Exit(1)
	}

	fmt.Printf("starting the cluster with %d nodes…\n", *nodes)
	if err := stack.Up(ctx, *nodes); err != nil {
		fmt.Fprintln(os.Stderr, "could not start the cluster:", err)
		fmt.Fprintln(os.Stderr, stack.Logs(ctx, "gateway", 40))
		_ = stack.Down(ctx)
		os.Exit(1)
	}
	fmt.Println("cluster is up at", stack.BaseURL)

	code := godog.TestSuite{
		Name:                "ryuk",
		ScenarioInitializer: InitializeScenario,
		Options:             &opts,
	}.Run()

	if m.Run() != 0 {
		code = 1
	}

	if *keepUp {
		fmt.Println("leaving the cluster up; stop it with: docker compose -p ryuk-it down -v")
	} else {
		fmt.Println("stopping the cluster…")
		_ = stack.Down(context.Background())
	}
	os.Exit(code)
}

func clientFor(org string) *client.Client {
	return client.New(stack.BaseURL, org+"-token")
}
