package bms.player.beatoraja;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.util.*;
import java.util.logging.Logger;

import bms.model.BMSModel;
import bms.player.lunaticrave2.LunaticRave2SongDatabaseManager;
import bms.table.DifficultyTable;
import bms.table.DifficultyTableElement;
import bms.table.DifficultyTableParser;
import bms.table.DifficultyTable.Grade;

import com.badlogic.gdx.utils.Json;
import com.badlogic.gdx.utils.JsonWriter.OutputType;

import javafx.application.Platform;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.*;
import javafx.scene.layout.GridPane;
import javafx.stage.DirectoryChooser;
import javafx.util.Callback;

/**
 * Beatorajaの設定ダイアログ
 * 
 * @author exch
 */
public class PlayConfigurationView implements Initializable {

	/**
	 * ハイスピード
	 */
	@FXML
	private Spinner<Double> hispeed;

	@FXML
	private GridPane lr2configuration;
	@FXML
	private CheckBox fixgvalue;
	@FXML
	private Spinner<Integer> gvalue;
	@FXML
	private CheckBox fullscreen;
	@FXML
	private CheckBox vsync;
	@FXML
	private ComboBox<Integer> bgaop;

	@FXML
	private ListView<String> bmsroot;
	@FXML
	private TextField url;
	@FXML
	private ListView<String> tableurl;

	@FXML
	private ComboBox<Integer> scoreop;
	@FXML
	private ComboBox<Integer> gaugeop;
	@FXML
	private ComboBox<String> configBox;
	@FXML
	private CheckBox enableLanecover;
	@FXML
	private Spinner<Double> lanecover;
	@FXML
	private CheckBox enableLift;
	@FXML
	private Spinner<Double> lift;

	@FXML
	private TextField vlcpath;

	@FXML
	private Spinner<Integer> judgetiming;
	@FXML
	private CheckBox constant;
	@FXML
	private CheckBox bpmguide;
	@FXML
	private CheckBox legacy;

	@FXML
	private Spinner<Integer> maxfps;
	@FXML
	private Spinner<Integer> audiobuffer;
	@FXML
	private Spinner<Integer> audiosim;
	@FXML
	private ComboBox<Integer> judgealgorithm;

	private Config config;;

	private static final String[] SCOREOP = { "OFF", "MIRROR", "RANDOM",
			"R-RANDOM", "S-RANDOM", "SPIRAL", "H-RANDOM", "ALL-SCR",
			"RANDOM-EX", "S-RANDOM-EX" };

	private static final String[] GAUGEOP = { "ASSIST EASY", "EASY", "NORMAL",
			"HARD", "EX-HARD", "HAZARD" };

	private static final String[] BGAOP = { "ON", "AUTOPLAY ", "OFF" };

	private static final String[] JUDGEALGORITHM = { "LR2風", "本家風", "最下ノーツ最優先" };

	public void initialize(URL arg0, ResourceBundle arg1) {
		lr2configuration.setHgap(25);
		lr2configuration.setVgap(4);

		scoreop.setCellFactory(new Callback<ListView<Integer>, ListCell<Integer>>() {
			public ListCell<Integer> call(ListView<Integer> param) {
				return new OptionListCell(SCOREOP);
			}
		});
		scoreop.setButtonCell(new OptionListCell(SCOREOP));
		scoreop.getItems().setAll(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
		gaugeop.setCellFactory(new Callback<ListView<Integer>, ListCell<Integer>>() {
			public ListCell<Integer> call(ListView<Integer> param) {
				return new OptionListCell(GAUGEOP);
			}
		});
		gaugeop.setButtonCell(new OptionListCell(GAUGEOP));
		gaugeop.getItems().setAll(0, 1, 2, 3, 4, 5);
		bgaop.setCellFactory(new Callback<ListView<Integer>, ListCell<Integer>>() {
			public ListCell<Integer> call(ListView<Integer> param) {
				return new OptionListCell(BGAOP);
			}
		});
		bgaop.setButtonCell(new OptionListCell(BGAOP));
		bgaop.getItems().setAll(0, 1, 2);
		judgealgorithm.setButtonCell(new OptionListCell(JUDGEALGORITHM));
		judgealgorithm.getItems().setAll(0, 1, 2);
	}

	/**
	 * ダイアログの項目を更新する
	 */
	public void update(Config config) {
		this.config = config;
		fullscreen.setSelected(config.isFullscreen());
		vsync.setSelected(config.isVsync());
		bgaop.setValue(config.getBga());
		scoreop.getSelectionModel().select(config.getRandom());
		gaugeop.getSelectionModel().select(config.getGauge());

		fixgvalue.setSelected(config.isFixhispeed());
		hispeed.getValueFactory().setValue((double) config.getHispeed());
		gvalue.getValueFactory().setValue(config.getGreenvalue());
		enableLanecover.setSelected(config.isEnablelanecover());
		lanecover.getValueFactory().setValue((double) config.getLanecover());
		enableLift.setSelected(config.isEnablelift());
		lift.getValueFactory().setValue((double) config.getLift());
		judgetiming.getValueFactory().setValue(config.getJudgetiming());

		vlcpath.setText(config.getVlcpath());

		bmsroot.getItems().setAll(config.getBmsroot());
		tableurl.getItems().setAll(config.getTableURL());

		constant.setSelected(config.isConstant());
		bpmguide.setSelected(config.isBpmguide());
		legacy.setSelected(config.getLnassist() == 1);

		maxfps.getValueFactory().setValue(config.getMaxFramePerSecond());
		audiobuffer.getValueFactory().setValue(
				config.getAudioDeviceBufferSize());
		audiosim.getValueFactory().setValue(
				config.getAudioDeviceSimultaneousSources());

		judgealgorithm.setValue(config.getJudgeAlgorithm());
	}

	/**
	 * ダイアログの項目をconfig.xmlに反映する
	 */
	public void commit() {
		config.setFullscreen(fullscreen.isSelected());
		config.setVsync(vsync.isSelected());
		config.setBga(bgaop.getValue());
		config.setRandom(scoreop.getValue());
		config.setGauge(gaugeop.getValue());
		config.setHispeed(hispeed.getValue().floatValue());
		config.setFixhispeed(fixgvalue.isSelected());
		config.setGreenvalue(gvalue.getValue());
		config.setEnablelanecover(enableLanecover.isSelected());
		config.setLanecover(lanecover.getValue().floatValue());
		config.setEnablelift(enableLift.isSelected());
		config.setLift(lift.getValue().floatValue());
		config.setJudgetiming(judgetiming.getValue());

		config.setVlcpath(vlcpath.getText());

		config.setBmsroot(bmsroot.getItems().toArray(new String[0]));
		config.setTableURL(tableurl.getItems().toArray(new String[0]));

		config.setConstant(constant.isSelected());
		config.setBpmguide(bpmguide.isSelected());
		config.setLnassist(legacy.isSelected() ? 1 : 0);

		config.setMaxFramePerSecond(maxfps.getValue());
		config.setAudioDeviceBufferSize(audiobuffer.getValue());
		config.setAudioDeviceSimultaneousSources(audiosim.getValue());

		config.setJudgeAlgorithm(judgealgorithm.getValue());

		Json json = new Json();
		json.setOutputType(OutputType.json);
		try {
			FileWriter fw = new FileWriter("config.json");
			fw.write(json.prettyPrint(config));
			fw.flush();
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void addSongPath() {
		DirectoryChooser chooser = new DirectoryChooser();
		chooser.setTitle("楽曲のルートフォルダを選択してください");
		File dir = chooser.showDialog(null);
		if (dir != null) {
			bmsroot.getItems().add(dir.getPath());
		}
	}

	public void removeSongPath() {
		bmsroot.getItems().removeAll(
				bmsroot.getSelectionModel().getSelectedItems());
	}

	public void addTableURL() {
		String s = url.getText();
		if(s.startsWith("http") && !tableurl.getItems().contains(s)) {
			tableurl.getItems().add(url.getText());
		}
	}

	public void removeTableURL() {
		tableurl.getItems().removeAll(
				tableurl.getSelectionModel().getSelectedItems());
	}

	public void start() {
		commit();
		Platform.exit();
		MainController.play(null, 0, true);
	}

	public void loadBMS() {
		commit();
		try {
			Class.forName("org.sqlite.JDBC");
			LunaticRave2SongDatabaseManager songdb = new LunaticRave2SongDatabaseManager(
					new File("song.db").getPath(), true,
					BMSModel.LNTYPE_CHARGENOTE);
			songdb.createTable();
			Logger.getGlobal().info("song.db更新開始");
			File[] files = new File[config.getBmsroot().length];
			for (int i = 0; i < files.length; i++) {
				files[i] = new File(config.getBmsroot()[i]);
			}
			songdb.updateSongDatas(files, config.getBmsroot(),
					new File(".").getAbsolutePath());
			Logger.getGlobal().info("song.db更新完了");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	public void loadTable() {
		commit();
		File dir = new File("table");
		if(!dir.exists()) {
			dir.mkdir();
		}
		for(File f : dir.listFiles()) {
			f.delete();
		}
		for (String url : config.getTableURL()) {
			DifficultyTableParser dtp = new DifficultyTableParser();
			DifficultyTable dt = new DifficultyTable();
			dt.setSourceURL(url);
			try {
				dtp.decode(true, dt);
				TableData td = new TableData();
				td.setName(dt.getName());
				td.setLevel(dt.getLevelDescription());
				HashMap<String, String[]> levels = new HashMap<String, String[]>();
				for (String lv : dt.getLevelDescription()) {
					List<String> hashes = new ArrayList<String>();
					for (DifficultyTableElement dte : dt.getElements()) {
						if (lv.equals(dte.getDifficultyID())) {
							hashes.add(dte.getHash());
						}
					}
					levels.put(lv, hashes.toArray(new String[0]));
				}
				td.setHash(levels);

				if (dt.getGrade() != null) {
					List<String> gname = new ArrayList();
					HashMap<String, String[]> l = new HashMap();
					for (Grade g : dt.getGrade()) {
						gname.add(g.getName());
						l.put(g.getName(), g.getHashes());
					}
					td.setGrade(gname.toArray(new String[0]));
					td.setGradehash(l);
				}
				Json json = new Json();
				json.setElementType(TableData.class, "hash", HashMap.class);
				json.setElementType(TableData.class, "gradehash", HashMap.class);
				json.setOutputType(OutputType.json);
				FileWriter fw = new FileWriter("table/" + td.getName()
						+ ".json");
				fw.write(json.prettyPrint(td));
				fw.flush();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void exit() {
		commit();
		Platform.exit();
		System.exit(0);
	}

	class OptionListCell extends ListCell<Integer> {

		private final String[] strings;

		public OptionListCell(String[] strings) {
			this.strings = strings;
		}

		@Override
		protected void updateItem(Integer arg0, boolean arg1) {
			super.updateItem(arg0, arg1);
			if (arg0 != null) {
				setText(strings[arg0]);
			}
		}
	}

}
