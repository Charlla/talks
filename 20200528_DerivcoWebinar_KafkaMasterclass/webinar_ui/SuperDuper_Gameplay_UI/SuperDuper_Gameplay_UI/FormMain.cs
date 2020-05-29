using SuperDuper_Gameplay_UI.Classes;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace SuperDuper_Gameplay_UI
{
    public partial class FormMain : Form
    {
        private readonly SynchronizationContext synchronizationContext;
        private DateTime previousTime = DateTime.Now;
        private DateTime previousTimeTN = DateTime.Now;
        CancellationTokenSource cancellationToken = new CancellationTokenSource();
        public FormMain()
        {
            InitializeComponent();
            synchronizationContext = SynchronizationContext.Current;
        }

        private void buttonStart_Click(object sender, EventArgs e)
        {
            KafkaConsumer.Instance.StartConsumer(cancellationToken, this);
        }

        public void UpdateLabels(Dictionary<string, string> stats)
        {
            var timeNow = DateTime.Now;

            if ((DateTime.Now - previousTimeTN).Ticks <= 5) return;


            synchronizationContext.Post(new SendOrPostCallback(o =>
            {
                foreach (var stat in stats)
                {
                    try
                    {
                        string value = "-";
                        if (stat.Value.Contains("."))
                        {
                            value = stat.Value.Substring(0, stat.Value.IndexOf("."));
                        }
                        else
                        {
                            value = stat.Value;
                        }
                        ((Label)this.Controls.Find(stat.Key, true).FirstOrDefault()).Text = value;
                    }
                    catch (Exception ex)
                    {

                    }
                }
            }), stats);

            previousTimeTN = timeNow;
        }

        private void buttonStop_Click(object sender, EventArgs e)
        {
            cancellationToken.Cancel();
        }

        private void groupBox9_Enter(object sender, EventArgs e)
        {

        }
    }
}
